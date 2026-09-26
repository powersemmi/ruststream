//! Brokers that each break one lifecycle rule, and the correct ones: every check here is proved by
//! a broker it fails and a broker it passes.
//!
//! [`Faulty`] is the in-memory bus with one [`Fault`] switched on; [`Queue`] is a small queue that
//! keeps messages across connections, for the [`Backlog::Delivered`] half of
//! [`shutdown_flushes`].

use std::{
    collections::{HashMap, VecDeque},
    future::{Future, pending, ready},
    mem::take,
    pin::pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::{Notify, mpsc, oneshot},
    task::AbortHandle,
    time::sleep,
};

use super::{CONSTRUCTION_BUDGET, shared_handle_closes, shutdown_flushes};
use crate::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Lend, NamedCopies,
    OutgoingMessage, Publisher, Subscribe, Subscriber, SubscriptionSource,
    conformance::harness::lifecycle,
    memory::{
        ClosedMemoryBroker, ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage,
        MemoryPublisher, MemorySubscriber,
    },
    testing::Backlog,
};

/// The one rule a [`Faulty`] broker breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Breaks nothing.
    None,
    /// The constructor spawns a task, so it needs a runtime.
    SpawnsInConstructor,
    /// The constructor waits, the way one that dials a server does.
    BlocksInConstructor,
    /// The subscription is fed by a task spawned on the subscribing caller's runtime.
    SubscribesOnCaller,
    /// The publisher attaches its connection on the first caller's runtime; later publishes are
    /// handed to it and forgotten.
    AttachesOnFirstCaller,
    /// `nack(requeue = true)` runs on the settling caller's runtime.
    RequeuesOnCaller,
    /// `nack(requeue = false)` runs on the settling caller's runtime, and a delivery nobody
    /// settled comes back.
    DropsOnCaller,
    /// `nack_after` rounds its delay down to whole seconds.
    RoundsDelayDown,
    /// `shutdown` never returns.
    ShutdownHangs,
    /// A publisher attaches its connection on first use, and attaches after shutdown too.
    UntouchedReconnects,
    /// `nack` after shutdown answers `Ok` and does nothing.
    ClaimsAfterShutdown,
    /// A publish is handed to a task that shutdown stops without flushing it.
    DefersPublishes,
    /// A subscription opened after shutdown stays open and silent.
    SubscribesAfterShutdown,
}

/// The in-memory bus with one fault switched on.
#[derive(Clone)]
struct Faulty {
    bus: MemoryBroker,
    fault: Fault,
}

impl Faulty {
    fn new(fault: Fault) -> Self {
        Self::over(MemoryBroker::new(), fault)
    }

    /// A broker over `bus`, so every one built this way reaches the same messages.
    fn over(bus: MemoryBroker, fault: Fault) -> Self {
        match fault {
            Fault::SpawnsInConstructor => drop(tokio::spawn(async {})),
            Fault::BlocksInConstructor => {
                thread::sleep(CONSTRUCTION_BUDGET + Duration::from_millis(200));
            }
            _ => {}
        }
        Self { bus, fault }
    }
}

impl Broker for Faulty {
    type Error = MemoryError;
    type Connected = ConnectedFaulty;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let inner = self.bus.clone().connect().await?;
        Ok(ConnectedFaulty {
            inner,
            bus: self.bus,
            fault: self.fault,
            closed: Arc::default(),
            runtime: Handle::current(),
            deferred: Arc::default(),
        })
    }
}

#[derive(Clone)]
struct ConnectedFaulty {
    inner: ConnectedMemoryBroker,
    bus: MemoryBroker,
    fault: Fault,
    closed: Arc<AtomicBool>,
    runtime: Handle,
    deferred: Arc<Mutex<Vec<AbortHandle>>>,
}

impl ConnectedFaulty {
    fn publisher(&self) -> FaultyPublisher {
        FaultyPublisher {
            fault: self.fault,
            bus: self.bus.clone(),
            connected: self.inner.clone(),
            paired: self.inner.publisher(),
            lazy: OnceLock::new(),
            socket: Mutex::new(None),
            closed: Arc::clone(&self.closed),
            runtime: self.runtime.clone(),
            deferred: Arc::clone(&self.deferred),
        }
    }
}

impl ConnectedBroker for ConnectedFaulty {
    type Error = MemoryError;
    type Closed = ClosedMemoryBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        if self.fault == Fault::ShutdownHangs {
            pending::<()>().await;
        }
        self.closed.store(true, Ordering::SeqCst);
        let deferred: Vec<_> = self.deferred.lock().expect("poisoned").drain(..).collect();
        for task in deferred {
            task.abort();
        }
        self.inner.shutdown().await
    }
}

/// What a publish hands the connection a [`Fault::AttachesOnFirstCaller`] publisher attached.
type Frame = (String, Bytes, HeaderMap, Option<oneshot::Sender<()>>);

struct FaultyPublisher {
    fault: Fault,
    bus: MemoryBroker,
    connected: ConnectedMemoryBroker,
    paired: MemoryPublisher,
    lazy: OnceLock<MemoryPublisher>,
    socket: Mutex<Option<mpsc::UnboundedSender<Frame>>>,
    closed: Arc<AtomicBool>,
    runtime: Handle,
    deferred: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Publisher for FaultyPublisher {
    type Payload = Lend;
    type Error = MemoryError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let name = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let headers = msg.headers().clone();
        match self.fault {
            Fault::UntouchedReconnects => {
                let publisher = if let Some(publisher) = self.lazy.get() {
                    publisher
                } else {
                    // Connecting a clone of a shut-down bus revives it: the lazy attach reaches
                    // a connection of its own.
                    let attached = self.bus.clone().connect().await?;
                    self.lazy.get_or_init(|| attached.publisher())
                };
                publisher
                    .publish(
                        OutgoingMessage::new(&name, &payload).with_headers(headers),
                        None,
                    )
                    .await
            }
            Fault::AttachesOnFirstCaller => {
                let (socket, first) = {
                    let mut socket = self.socket.lock().expect("poisoned");
                    let first = socket.is_none();
                    let sender = socket.get_or_insert_with(|| {
                        let (frames, mut incoming) = mpsc::unbounded_channel::<Frame>();
                        let publisher = self.connected.publisher();
                        drop(tokio::spawn(async move {
                            while let Some((name, payload, headers, attached)) =
                                incoming.recv().await
                            {
                                let _ = publisher
                                    .publish(
                                        OutgoingMessage::new(&name, &payload).with_headers(headers),
                                        None,
                                    )
                                    .await;
                                if let Some(attached) = attached {
                                    let _ = attached.send(());
                                }
                            }
                        }));
                        frames
                    });
                    let sender = sender.clone();
                    drop(socket);
                    (sender, first)
                };
                if first {
                    let (attached, handshake) = oneshot::channel();
                    let _ = socket.send((name, payload, headers, Some(attached)));
                    let _ = handshake.await;
                } else {
                    let _ = socket.send((name, payload, headers, None));
                }
                Ok(())
            }
            Fault::DefersPublishes => {
                if self.closed.load(Ordering::SeqCst) {
                    return Err(MemoryError::ShutDown);
                }
                let connected = self.connected.clone();
                let task = self.runtime.spawn(async move {
                    sleep(Duration::from_millis(50)).await;
                    let _ = connected
                        .publisher()
                        .publish(
                            OutgoingMessage::new(&name, &payload).with_headers(headers),
                            None,
                        )
                        .await;
                });
                self.deferred
                    .lock()
                    .expect("poisoned")
                    .push(task.abort_handle());
                Ok(())
            }
            _ => {
                self.paired
                    .publish(
                        OutgoingMessage::new(&name, &payload).with_headers(headers),
                        None,
                    )
                    .await
            }
        }
    }
}

/// The descriptor of a [`Faulty`] subscription.
#[derive(Clone)]
struct FaultySource(String);

type MemoryStreamError = <MemorySubscriber as Subscriber>::Error;

impl SubscriptionSource<ConnectedFaulty> for FaultySource {
    type Subscriber = FaultySubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        &self.0
    }

    async fn subscribe(self, connected: &ConnectedFaulty) -> Result<FaultySubscriber, MemoryError> {
        let feed = if connected.fault == Fault::SubscribesAfterShutdown
            && connected.closed.load(Ordering::SeqCst)
        {
            let (open, silent) = mpsc::unbounded_channel();
            Feed::Silent {
                pumped: silent,
                _open: open,
            }
        } else {
            let inner = Subscribe::subscribe(&connected.inner, &self.0).await?;
            if connected.fault == Fault::SubscribesOnCaller {
                let (forward, pumped) = mpsc::unbounded_channel();
                drop(tokio::spawn(async move {
                    let mut inner = inner;
                    let mut deliveries = pin!(inner.stream());
                    while let Some(item) = deliveries.next().await {
                        if forward.send(item).is_err() {
                            break;
                        }
                    }
                }));
                Feed::Pumped(pumped)
            } else {
                Feed::Direct(inner)
            }
        };
        Ok(FaultySubscriber {
            feed,
            fault: connected.fault,
            closed: Arc::clone(&connected.closed),
        })
    }
}

type Pumped = mpsc::UnboundedReceiver<Result<MemoryMessage, MemoryStreamError>>;

enum Feed {
    Direct(MemorySubscriber),
    Pumped(Pumped),
    /// Held open by a sender nobody sends on.
    Silent {
        pumped: Pumped,
        _open: mpsc::UnboundedSender<Result<MemoryMessage, MemoryStreamError>>,
    },
}

struct FaultySubscriber {
    feed: Feed,
    fault: Fault,
    closed: Arc<AtomicBool>,
}

impl Subscriber for FaultySubscriber {
    type Message = FaultyMessage;
    type Error = MemoryStreamError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let fault = self.fault;
        let closed = Arc::clone(&self.closed);
        let wrap = move |item: Result<MemoryMessage, MemoryStreamError>| {
            item.map(|inner| FaultyMessage {
                inner: Some(inner),
                fault,
                closed: Arc::clone(&closed),
            })
        };
        match &mut self.feed {
            Feed::Direct(inner) => inner.stream().map(wrap).left_stream(),
            Feed::Pumped(pumped) | Feed::Silent { pumped, .. } => {
                stream::poll_fn(move |cx| pumped.poll_recv(cx))
                    .map(wrap)
                    .right_stream()
            }
        }
    }
}

struct FaultyMessage {
    inner: Option<MemoryMessage>,
    fault: Fault,
    closed: Arc<AtomicBool>,
}

impl FaultyMessage {
    fn take(&mut self) -> MemoryMessage {
        self.inner.take().expect("a delivery is settled once")
    }
}

/// Puts a delivery nobody settled back on its queue when dropped.
struct RequeueOnDrop(Option<MemoryMessage>);

impl Drop for RequeueOnDrop {
    fn drop(&mut self) {
        if let Some(unsettled) = self.0.take() {
            // The in-memory requeue happens in the call; the future only carries its answer.
            let _requeued = unsettled.nack(true);
        }
    }
}

/// Consumes the delivery it holds when it is dropped: the stand-in for work that is lost with the
/// runtime it was left on, now that an unsettled memory delivery goes back to its subscription.
struct ConsumeOnDrop(Option<MemoryMessage>);

impl Drop for ConsumeOnDrop {
    fn drop(&mut self) {
        if let Some(unsettled) = self.0.take() {
            // The in-memory settlement happens in the call; the future only carries its answer.
            let _consumed = unsettled.nack(false);
        }
    }
}

impl IncomingMessage for FaultyMessage {
    fn payload(&self) -> &[u8] {
        self.inner.as_ref().map_or(&[], IncomingMessage::payload)
    }

    fn headers(&self) -> &HeaderMap {
        self.inner
            .as_ref()
            .expect("an unsettled delivery")
            .headers()
    }

    async fn ack(mut self) -> Result<(), AckError> {
        self.take().ack().await
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let inner = self.take();
        match self.fault {
            Fault::ClaimsAfterShutdown if self.closed.load(Ordering::SeqCst) => {
                drop(ConsumeOnDrop(Some(inner)));
                Ok(())
            }
            Fault::RequeuesOnCaller if requeue => {
                let guard = ConsumeOnDrop(Some(inner));
                drop(tokio::spawn(async move {
                    let mut guard = guard;
                    if let Some(inner) = guard.0.take() {
                        let _ = inner.nack(true).await;
                    }
                }));
                Ok(())
            }
            Fault::DropsOnCaller if !requeue => {
                let guard = RequeueOnDrop(Some(inner));
                drop(tokio::spawn(async move {
                    let mut guard = guard;
                    if let Some(inner) = guard.0.take() {
                        let _ = inner.nack(false).await;
                    }
                }));
                Ok(())
            }
            _ => inner.nack(requeue).await,
        }
    }

    fn supports_nack_after(&self) -> bool {
        true
    }

    async fn nack_after(mut self, delay: Duration) -> Result<(), AckError> {
        let delay = if self.fault == Fault::RoundsDelayDown {
            Duration::from_secs(delay.as_secs())
        } else {
            delay
        };
        self.take().nack_after(delay).await
    }
}

async fn ladder_on(fault: Fault) {
    lifecycle(
        move || Faulty::new(fault),
        |name| FaultySource(name.to_owned()),
        |connected: &ConnectedFaulty| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_follows_the_ladder_passes() {
    ladder_on(Fault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "constructor panicked on a thread with no Tokio runtime")]
async fn a_constructor_that_spawns_fails() {
    ladder_on(Fault::SpawnsInConstructor).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the broker constructor took")]
async fn a_constructor_that_waits_fails() {
    ladder_on(Fault::BlocksInConstructor).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a subscription opened from a runtime that stopped must keep receiving")]
async fn a_subscription_fed_from_the_callers_runtime_fails() {
    ladder_on(Fault::SubscribesOnCaller).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a publisher whose first publish came from a runtime that stopped")]
async fn a_connection_attached_on_the_first_callers_runtime_fails() {
    ladder_on(Fault::AttachesOnFirstCaller).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack(requeue = true) from a runtime that stopped answered Ok")]
async fn a_requeue_run_on_the_callers_runtime_fails() {
    ladder_on(Fault::RequeuesOnCaller).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "dropped with nack(requeue = false) from a runtime that stopped came back"
)]
async fn a_drop_run_on_the_callers_runtime_fails() {
    ladder_on(Fault::DropsOnCaller).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a broker that ignores the delay or rounds it down")]
async fn a_delay_rounded_down_fails() {
    ladder_on(Fault::RoundsDelayDown).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: shutdown hung")]
async fn a_shutdown_that_hangs_fails() {
    ladder_on(Fault::ShutdownHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a publisher paired before shutdown and never used succeeded")]
async fn a_publisher_attaching_after_shutdown_fails() {
    ladder_on(Fault::UntouchedReconnects).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "held across shutdown answered Ok to nack(requeue = true) and never")]
async fn a_settlement_claimed_after_shutdown_fails() {
    ladder_on(Fault::ClaimsAfterShutdown).await;
}

async fn flush_on(fault: Fault) {
    let bus = MemoryBroker::new();
    shutdown_flushes(
        move || Faulty::over(bus.clone(), fault),
        |name| FaultySource(name.to_owned()),
        |connected: &ConnectedFaulty| connected.publisher(),
        Backlog::Missed,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_subscribe_broker_that_flushes_passes() {
    flush_on(Fault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "never reached a subscription open on another connection")]
async fn a_publish_dropped_by_shutdown_fails_on_publish_subscribe() {
    flush_on(Fault::DefersPublishes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "shutdown_flushes: shutdown hung")]
async fn a_shutdown_that_hangs_fails_the_flush() {
    flush_on(Fault::ShutdownHangs).await;
}

async fn shared_on(fault: Fault) {
    shared_handle_closes(
        move || Faulty::new(fault),
        |name| FaultySource(name.to_owned()),
        |connected: &ConnectedFaulty| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clones_that_close_with_the_original_pass() {
    shared_on(Fault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a publisher paired from the clone before shutdown succeeded")]
async fn a_clone_that_reconnects_fails() {
    shared_on(Fault::UntouchedReconnects).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "stayed open and silent")]
async fn a_clone_that_opens_a_silent_subscription_fails() {
    shared_on(Fault::SubscribesAfterShutdown).await;
}

/// The rule a [`Queue`] breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueFault {
    /// Breaks nothing.
    None,
    /// An acknowledgement answers `Ok` and waits for a flush the shutdown never runs.
    LosesAcks,
    /// A publish answers `Ok` and waits for a flush the shutdown never runs: the next pull of the
    /// connection's subscription flushes it.
    LosesPublishes,
    /// An acknowledgement after shutdown answers `Ok` and does nothing.
    ClaimsAckAfterShutdown,
}

#[derive(Debug, Error)]
#[error("the queue connection is closed")]
struct LinkClosed;

/// Messages every connection to one [`Queue`] shares.
#[derive(Default)]
struct World {
    queues: Mutex<HashMap<String, VecDeque<Bytes>>>,
    arrived: Notify,
}

impl World {
    fn push(&self, name: &str, payload: Bytes, front: bool) {
        let mut queues = self.queues.lock().expect("poisoned");
        let queue = queues.entry(name.to_owned()).or_default();
        if front {
            queue.push_front(payload);
        } else {
            queue.push_back(payload);
        }
        drop(queues);
        self.arrived.notify_waiters();
    }

    fn pop(&self, name: &str) -> Option<Bytes> {
        self.queues
            .lock()
            .expect("poisoned")
            .get_mut(name)
            .and_then(VecDeque::pop_front)
    }
}

/// One connection's state: what it delivered and nobody settled yet.
#[derive(Default)]
struct Link {
    closed: AtomicBool,
    unacked: Mutex<Vec<(u64, String, Bytes)>>,
    next: Mutex<u64>,
    unflushed: Mutex<Vec<(String, Bytes)>>,
}

impl Link {
    fn settle(&self, id: u64) -> Option<(String, Bytes)> {
        let mut unacked = self.unacked.lock().expect("poisoned");
        let at = unacked.iter().position(|(held, ..)| *held == id)?;
        let (_, name, payload) = unacked.remove(at);
        drop(unacked);
        Some((name, payload))
    }
}

/// A queue that keeps its messages across connections and gives back what a closing connection
/// did not settle.
#[derive(Clone)]
struct Queue {
    world: Arc<World>,
    fault: QueueFault,
}

impl Broker for Queue {
    type Error = LinkClosed;
    type Connected = ConnectedQueue;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        ready(Ok(ConnectedQueue {
            world: self.world,
            link: Arc::default(),
            fault: self.fault,
        }))
    }
}

struct ConnectedQueue {
    world: Arc<World>,
    link: Arc<Link>,
    fault: QueueFault,
}

impl ConnectedBroker for ConnectedQueue {
    type Error = LinkClosed;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        self.link.closed.store(true, Ordering::SeqCst);
        let unacked = take(&mut *self.link.unacked.lock().expect("poisoned"));
        for (_, name, payload) in unacked.into_iter().rev() {
            self.world.push(&name, payload, true);
        }
        ready(Ok(()))
    }
}

struct QueuePublisher {
    world: Arc<World>,
    link: Arc<Link>,
    fault: QueueFault,
}

impl Publisher for QueuePublisher {
    type Payload = Lend;
    type Error = LinkClosed;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if self.link.closed.load(Ordering::SeqCst) {
            return ready(Err(LinkClosed));
        }
        let payload = Bytes::copy_from_slice(msg.payload());
        if self.fault == QueueFault::LosesPublishes {
            // Flushed by the next pull of this connection's subscription, which the shutdown
            // does not wait for.
            self.link
                .unflushed
                .lock()
                .expect("poisoned")
                .push((msg.name().to_owned(), payload));
            self.world.arrived.notify_waiters();
        } else {
            self.world.push(msg.name(), payload, false);
        }
        ready(Ok(()))
    }
}

#[derive(Clone)]
struct QueueSource(String);

impl SubscriptionSource<ConnectedQueue> for QueueSource {
    type Subscriber = QueueSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        &self.0
    }

    fn subscribe(
        self,
        connected: &ConnectedQueue,
    ) -> impl Future<Output = Result<QueueSubscriber, LinkClosed>> + Send {
        ready(if connected.link.closed.load(Ordering::SeqCst) {
            Err(LinkClosed)
        } else {
            Ok(QueueSubscriber {
                name: self.0,
                world: Arc::clone(&connected.world),
                link: Arc::clone(&connected.link),
                fault: connected.fault,
            })
        })
    }
}

struct QueueSubscriber {
    name: String,
    world: Arc<World>,
    link: Arc<Link>,
    fault: QueueFault,
}

impl Subscriber for QueueSubscriber {
    type Message = QueueMessage;
    type Error = LinkClosed;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::unfold(&*self, |this| async move {
            loop {
                let arrived = this.world.arrived.notified();
                let mut arrived = pin!(arrived);
                arrived.as_mut().enable();
                if this.link.closed.load(Ordering::SeqCst) {
                    return None;
                }
                let unflushed = take(&mut *this.link.unflushed.lock().expect("poisoned"));
                for (name, payload) in unflushed {
                    this.world.push(&name, payload, false);
                }
                if let Some(payload) = this.world.pop(&this.name) {
                    let id = {
                        let mut next = this.link.next.lock().expect("poisoned");
                        *next += 1;
                        *next
                    };
                    this.link.unacked.lock().expect("poisoned").push((
                        id,
                        this.name.clone(),
                        payload.clone(),
                    ));
                    let msg = QueueMessage {
                        id,
                        payload,
                        world: Arc::clone(&this.world),
                        link: Arc::clone(&this.link),
                        fault: this.fault,
                    };
                    return Some((Ok(msg), this));
                }
                arrived.await;
            }
        })
    }
}

struct QueueMessage {
    id: u64,
    payload: Bytes,
    world: Arc<World>,
    link: Arc<Link>,
    fault: QueueFault,
}

impl IncomingMessage for QueueMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        EMPTY.get_or_init(HeaderMap::new)
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        if self.link.closed.load(Ordering::SeqCst) {
            return ready(if self.fault == QueueFault::ClaimsAckAfterShutdown {
                Ok(())
            } else {
                Err(AckError::Broker(Box::new(LinkClosed)))
            });
        }
        if self.fault != QueueFault::LosesAcks {
            self.link.settle(self.id);
        }
        ready(Ok(()))
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        if self.link.closed.load(Ordering::SeqCst) {
            return ready(Err(AckError::Broker(Box::new(LinkClosed))));
        }
        if let Some((name, payload)) = self.link.settle(self.id)
            && requeue
        {
            self.world.push(&name, payload, true);
        }
        ready(Ok(()))
    }
}

async fn queue_flush_on(fault: QueueFault) {
    let world = Arc::new(World::default());
    shutdown_flushes(
        move || Queue {
            world: Arc::clone(&world),
            fault,
        },
        |name| QueueSource(name.to_owned()),
        |connected: &ConnectedQueue| QueuePublisher {
            world: Arc::clone(&connected.world),
            link: Arc::clone(&connected.link),
            fault: connected.fault,
        },
        Backlog::Delivered,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_that_flushes_passes() {
    queue_flush_on(QueueFault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "acknowledged right before shutdown came back")]
async fn a_queue_losing_acks_at_shutdown_fails() {
    queue_flush_on(QueueFault::LosesAcks).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "never reached a new connection")]
async fn a_queue_losing_publishes_at_shutdown_fails() {
    queue_flush_on(QueueFault::LosesPublishes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "acknowledged after shutdown answered Ok and came back")]
async fn a_queue_claiming_an_ack_after_shutdown_fails() {
    queue_flush_on(QueueFault::ClaimsAckAfterShutdown).await;
}
