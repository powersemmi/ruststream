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
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

use super::{CONSTRUCTION_BUDGET, ladder, shared_handle_closes, shutdown_flushes};
use crate::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Lend, NamedCopies,
    OutgoingMessage, Publisher, Subscribe, Subscriber, SubscriptionSource,
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
    /// The constructor never returns, the way one waiting on a server that never answers does.
    HangsInConstructor,
    /// `connect` fails.
    ConnectFails,
    /// `connect` never returns.
    ConnectHangs,
    /// Opening a subscription fails.
    SubscribeFails,
    /// Opening a subscription never returns.
    SubscribeHangs,
    /// Every publish fails.
    PublishFails,
    /// Every publish never returns.
    PublishHangs,
    /// A publish appends a byte to the payload.
    ManglesPayload,
    /// `nack_after` fails.
    NackAfterFails,
    /// `nack_after` never returns.
    NackAfterHangs,
    /// `nack_after` answers `Ok` and drops the delivery.
    NackAfterDrops,
    /// `ack` and `nack` never return.
    SettleHangs,
    /// `nack(requeue = true)` fails.
    RequeueFails,
    /// `nack(requeue = true)` answers `Unsupported` and drops the delivery, the way a transport
    /// with nothing to take back does.
    RequeueUnsupported,
    /// `nack(requeue = true)` answers `Ok`, drops the delivery and ends the subscription.
    RequeueEndsSubscription,
    /// `ack` fails.
    AckFails,
    /// `ack` answers `Ok` and hands the delivery back.
    AckRequeues,
    /// Like [`Fault::AttachesOnFirstCaller`], and a publish handed to the stopped connection
    /// fails.
    AttachesOnFirstCallerAndFails,
    /// `nack(requeue = true)` after shutdown answers `Ok` and hands the delivery back to its
    /// subscription seconds later.
    RequeuesLateAfterShutdown,
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
            Fault::HangsInConstructor => loop {
                thread::park();
            },
            _ => {}
        }
        Self { bus, fault }
    }
}

impl Broker for Faulty {
    type Error = MemoryError;
    type Connected = ConnectedFaulty;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        match self.fault {
            Fault::ConnectFails => return Err(MemoryError::ShutDown),
            Fault::ConnectHangs => pending::<()>().await,
            _ => {}
        }
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
            Fault::PublishFails => Err(MemoryError::ShutDown),
            Fault::PublishHangs => pending().await,
            Fault::ManglesPayload => {
                let mangled = [payload.as_ref(), b"~"].concat();
                self.paired
                    .publish(
                        OutgoingMessage::new(&name, mangled.as_slice()).with_headers(headers),
                        None,
                    )
                    .await
            }
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
            Fault::AttachesOnFirstCaller | Fault::AttachesOnFirstCallerAndFails => {
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
                } else if socket.send((name, payload, headers, None)).is_err()
                    && self.fault == Fault::AttachesOnFirstCallerAndFails
                {
                    return Err(MemoryError::ShutDown);
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
        match connected.fault {
            Fault::SubscribeFails => return Err(MemoryError::ShutDown),
            Fault::SubscribeHangs => pending::<()>().await,
            _ => {}
        }
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
            ended: Arc::default(),
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
    /// Ends the stream when notified.
    ended: Arc<Notify>,
}

impl Subscriber for FaultySubscriber {
    type Message = FaultyMessage;
    type Error = MemoryStreamError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let fault = self.fault;
        let closed = Arc::clone(&self.closed);
        let ended = Arc::clone(&self.ended);
        let wrap = {
            let ended = Arc::clone(&ended);
            move |item: Result<MemoryMessage, MemoryStreamError>| {
                item.map(|inner| FaultyMessage {
                    inner: Some(inner),
                    fault,
                    closed: Arc::clone(&closed),
                    ended: Arc::clone(&ended),
                })
            }
        };
        let deliveries = match &mut self.feed {
            Feed::Direct(inner) => inner.stream().map(wrap).left_stream(),
            Feed::Pumped(pumped) | Feed::Silent { pumped, .. } => {
                stream::poll_fn(move |cx| pumped.poll_recv(cx))
                    .map(wrap)
                    .right_stream()
            }
        };
        deliveries.take_until(async move { ended.notified().await })
    }
}

struct FaultyMessage {
    inner: Option<MemoryMessage>,
    fault: Fault,
    closed: Arc<AtomicBool>,
    /// Ends the subscription the delivery came from.
    ended: Arc<Notify>,
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
        let inner = self.take();
        match self.fault {
            Fault::SettleHangs => pending().await,
            Fault::AckFails => Err(AckError::Broker(Box::new(MemoryError::ShutDown))),
            Fault::AckRequeues => inner.nack(true).await,
            _ => inner.ack().await,
        }
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let inner = self.take();
        match self.fault {
            Fault::SettleHangs => pending().await,
            Fault::RequeueFails if requeue => {
                Err(AckError::Broker(Box::new(MemoryError::ShutDown)))
            }
            Fault::RequeueUnsupported if requeue => {
                drop(ConsumeOnDrop(Some(inner)));
                Err(AckError::Unsupported)
            }
            Fault::RequeueEndsSubscription if requeue => {
                drop(ConsumeOnDrop(Some(inner)));
                self.ended.notify_one();
                Ok(())
            }
            Fault::RequeuesLateAfterShutdown if requeue && self.closed.load(Ordering::SeqCst) => {
                // Longer than a quick look at the old subscription, well within a redelivery.
                drop(tokio::spawn(async move {
                    sleep(Duration::from_millis(2500)).await;
                    let _ = inner.nack(true).await;
                }));
                Ok(())
            }
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
        let inner = self.take();
        match self.fault {
            Fault::NackAfterFails => Err(AckError::Broker(Box::new(MemoryError::ShutDown))),
            Fault::NackAfterHangs => pending().await,
            Fault::NackAfterDrops => inner.nack(false).await,
            Fault::RoundsDelayDown => inner.nack_after(Duration::from_secs(delay.as_secs())).await,
            _ => inner.nack_after(delay).await,
        }
    }
}

/// The ladder alone: what the publisher carries is the message-shape suite's subject.
async fn ladder_on(fault: Fault) {
    ladder(
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
#[should_panic(expected = "the broker constructor did not return within")]
async fn a_constructor_that_waits_fails() {
    ladder_on(Fault::BlocksInConstructor).await;
}

/// A constructor that never returns is abandoned at the budget: the check fails with a panic that
/// names it instead of holding the run forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the broker constructor did not return within")]
async fn a_constructor_that_never_returns_fails() {
    ladder_on(Fault::HangsInConstructor).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: the broker must connect, got: ShutDown")]
async fn a_connect_that_fails_fails() {
    ladder_on(Fault::ConnectFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: connect hung")]
async fn a_connect_that_hangs_fails() {
    ladder_on(Fault::ConnectHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must open from a runtime of its own, got: ShutDown")]
async fn a_subscription_refused_fails() {
    ladder_on(Fault::SubscribeFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "from a runtime of its own hung")]
async fn a_subscription_that_never_opens_fails() {
    ladder_on(Fault::SubscribeHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: publish after connect failed: ShutDown")]
async fn a_publish_that_fails_fails() {
    ladder_on(Fault::PublishFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: publish after connect failed: the publish hung")]
async fn a_publish_that_hangs_fails() {
    ladder_on(Fault::PublishHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = r#"expected "lifecycle", got "lifecycle~""#)]
async fn a_publish_that_rewrites_the_payload_fails() {
    ladder_on(Fault::ManglesPayload).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delayed nack must be accepted, got: Broker")]
async fn a_delayed_nack_that_fails_fails() {
    ladder_on(Fault::NackAfterFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack_after from a runtime of its own hung")]
async fn a_delayed_nack_that_hangs_fails() {
    ladder_on(Fault::NackAfterHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delayed nack settled from another runtime never came back")]
async fn a_delayed_nack_that_drops_the_delivery_fails() {
    ladder_on(Fault::NackAfterDrops).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: nack(requeue = true) from a runtime of its own hung")]
async fn a_settlement_that_hangs_fails() {
    ladder_on(Fault::SettleHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "lifecycle: nack(requeue = true) must succeed or be unsupported, got: \
                           Broker"
)]
async fn a_requeue_that_fails_fails() {
    ladder_on(Fault::RequeueFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_cannot_requeue_passes() {
    ladder_on(Fault::RequeueUnsupported).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: waiting for the requeued message: the stream ended")]
async fn a_requeue_that_ends_the_subscription_fails() {
    ladder_on(Fault::RequeueEndsSubscription).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lifecycle: ack must succeed or be unsupported, got: Broker")]
async fn an_ack_that_fails_fails() {
    ladder_on(Fault::AckFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = r#"lifecycle: expected the repeated publishes, got "lifecycle""#)]
async fn an_ack_that_hands_the_delivery_back_fails() {
    ladder_on(Fault::AckRequeues).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "lifecycle: publish from the broker's runtime, through a publisher \
                           first used on a runtime that has since stopped, failed: ShutDown"
)]
async fn a_connection_attached_on_the_first_callers_runtime_that_errors_fails() {
    ladder_on(Fault::AttachesOnFirstCallerAndFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeue_after_shutdown_that_comes_back_late_passes() {
    ladder_on(Fault::RequeuesLateAfterShutdown).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_that_refuses_a_requeue_after_shutdown_passes() {
    queue_ladder_on(QueueFault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_taking_a_requeue_after_shutdown_passes() {
    queue_ladder_on(QueueFault::RequeuesAfterShutdown).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "shutdown_flushes: the subscription to")]
async fn a_subscription_refused_fails_the_flush() {
    flush_on(Fault::SubscribeFails).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "shutdown_flushes: ack must succeed or be unsupported, got: Broker")]
async fn an_ack_that_fails_fails_the_flush() {
    flush_on(Fault::AckFails).await;
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
#[should_panic(expected = "shut down stayed open (silent, or delivering)")]
async fn a_clone_that_opens_a_silent_subscription_fails() {
    shared_on(Fault::SubscribesAfterShutdown).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "publish through a publisher paired from the clone before shutdown hung")]
async fn a_clone_whose_publish_hangs_fails() {
    shared_on(Fault::PublishHangs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "shared_handle_closes: subscribing through the clone hung")]
async fn a_clone_whose_subscription_never_opens_fails() {
    shared_on(Fault::SubscribeHangs).await;
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
    /// An acknowledgement answers `Ok` and does nothing, and what a closing connection did not
    /// settle goes back only once its lease runs out, behind what was published meanwhile.
    LeasesLostAcks,
    /// `nack(requeue = true)` after shutdown takes effect over another channel: the delivery goes
    /// back on the queue.
    RequeuesAfterShutdown,
    /// Every subscription after the first one yields an error.
    FailsOnReconnect,
}

#[derive(Debug, Error)]
#[error("the queue connection is closed")]
struct LinkClosed;

/// Messages every connection to one [`Queue`] shares.
#[derive(Default)]
struct World {
    queues: Mutex<HashMap<String, VecDeque<Bytes>>>,
    arrived: Notify,
    subscriptions: AtomicUsize,
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
        let world = Arc::clone(&self.world);
        let give_back = move || {
            for (_, name, payload) in unacked.into_iter().rev() {
                world.push(&name, payload, true);
            }
        };
        if self.fault == QueueFault::LeasesLostAcks {
            // Past the quick look a reader once took after the published message arrived.
            drop(tokio::spawn(async move {
                sleep(Duration::from_millis(500)).await;
                give_back();
            }));
        } else {
            give_back();
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
                fails: connected.fault == QueueFault::FailsOnReconnect
                    && connected.world.subscriptions.fetch_add(1, Ordering::SeqCst) > 0,
                name: self.0,
                world: Arc::clone(&connected.world),
                link: Arc::clone(&connected.link),
                fault: connected.fault,
            })
        })
    }
}

struct QueueSubscriber {
    /// Yields an error instead of a delivery.
    fails: bool,
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
            if this.fails {
                return Some((Err(LinkClosed), this));
            }
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
                        name: this.name.clone(),
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
    name: String,
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
        if !matches!(
            self.fault,
            QueueFault::LosesAcks | QueueFault::LeasesLostAcks
        ) {
            self.link.settle(self.id);
        }
        ready(Ok(()))
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        if self.link.closed.load(Ordering::SeqCst) {
            return ready(
                if requeue && self.fault == QueueFault::RequeuesAfterShutdown {
                    self.world.push(&self.name, self.payload.clone(), true);
                    Ok(())
                } else {
                    Err(AckError::Broker(Box::new(LinkClosed)))
                },
            );
        }
        if let Some((name, payload)) = self.link.settle(self.id)
            && requeue
        {
            self.world.push(&name, payload, true);
        }
        ready(Ok(()))
    }
}

fn queue_publisher(connected: &ConnectedQueue) -> QueuePublisher {
    QueuePublisher {
        world: Arc::clone(&connected.world),
        link: Arc::clone(&connected.link),
        fault: connected.fault,
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
        queue_publisher,
        Backlog::Delivered,
    )
    .await;
}

/// The ladder over a queue: its deliveries offer no delayed nack, and every connection reaches
/// the same messages.
async fn queue_ladder_on(fault: QueueFault) {
    let world = Arc::new(World::default());
    ladder(
        move || Queue {
            world: Arc::clone(&world),
            fault,
        },
        |name| QueueSource(name.to_owned()),
        queue_publisher,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "acknowledged right before shutdown came back")]
async fn a_queue_losing_acks_that_return_behind_the_publish_fails() {
    queue_flush_on(QueueFault::LeasesLostAcks).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "shutdown_flushes: reading from another connection: the stream yielded an error"
)]
async fn a_new_connection_whose_stream_fails_fails_the_flush() {
    queue_flush_on(QueueFault::FailsOnReconnect).await;
}
