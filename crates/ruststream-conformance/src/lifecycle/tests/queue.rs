//! A small queue that keeps messages across connections, for the [`Backlog::Delivered`] half of
//! [`shutdown_flushes`].

use std::{
    collections::{HashMap, VecDeque},
    future::ready,
    mem::take,
    pin::pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, stream};
use thiserror::Error;
use tokio::{sync::Notify, time::sleep};

use crate::lifecycle::{ladder, shutdown_flushes};
use ruststream_core::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Lend, NamedCopies,
    OutgoingMessage, Publisher, Subscriber, SubscriptionSource,
};
use ruststream_runtime::testing::Backlog;

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
        stream::unfold(&*self, async move |this| {
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
async fn a_queue_that_refuses_a_requeue_after_shutdown_passes() {
    queue_ladder_on(QueueFault::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_taking_a_requeue_after_shutdown_passes() {
    queue_ladder_on(QueueFault::RequeuesAfterShutdown).await;
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
