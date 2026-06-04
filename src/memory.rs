//! In-process broker that keeps every message in memory.
//!
//! [`MemoryBroker`] implements [`Broker`] with broadcast semantics: each subscriber receives a
//! copy of every message published to its topic after the subscription was opened. There is no
//! durability, no consumer-group routing, and no on-disk state.
//!
//! It is a real, usable broker for single-process applications, prototypes, examples, and
//! local development, as well as the reference implementation the [`crate::conformance`]
//! harness runs against. It does not model any broker-specific semantics (`JetStream` ack
//! timing, `Kafka` offsets, `RabbitMQ` exchanges); for those, use the corresponding broker
//! crate.

use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use crate::{
    AckError, BatchPublisher, Broker, Headers, IncomingMessage, OutgoingMessage, Publisher,
    RawMessage, Subscribe, Subscriber, testing::TestClient,
};
use bytes::Bytes;
use futures::Stream;
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

type Sender = mpsc::UnboundedSender<MemoryDelivery>;

#[derive(Clone)]
struct MemoryDelivery {
    topic: String,
    payload: Bytes,
    headers: Headers,
}

#[derive(Default)]
struct MemoryState {
    subscribers: Mutex<HashMap<String, Vec<Sender>>>,
    published: Mutex<HashMap<String, Vec<RawMessage>>>,
    notify: Notify,
}

impl MemoryState {
    fn register(&self, topic: String, tx: Sender) {
        let mut subs = self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        subs.entry(topic).or_default().push(tx);
    }

    fn fanout(&self, delivery: &MemoryDelivery) {
        let snapshot = RawMessage::new(delivery.topic.clone(), delivery.payload.clone())
            .with_headers(delivery.headers.clone());
        {
            let mut log = self.published.lock().expect("memory broker mutex poisoned");
            log.entry(delivery.topic.clone())
                .or_default()
                .push(snapshot);
        }
        self.notify.notify_waiters();

        let subs = self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        if let Some(senders) = subs.get(&delivery.topic) {
            for tx in senders {
                let _ = tx.send(delivery.clone());
            }
        }
    }
}

/// An in-memory reference broker. Cheap to clone.
#[derive(Clone, Default)]
pub struct MemoryBroker {
    state: Arc<MemoryState>,
}

impl MemoryBroker {
    /// Creates a new empty broker. Equivalent to [`MemoryBroker::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens a subscription to `topic`. The returned subscriber starts receiving messages
    /// published after this call; messages published earlier are not buffered.
    #[must_use]
    pub fn subscribe(&self, topic: impl Into<String>) -> MemorySubscriber {
        let (tx, rx) = mpsc::unbounded_channel();
        let topic = topic.into();
        self.state.register(topic.clone(), tx.clone());
        MemorySubscriber {
            topic,
            rx: Some(rx),
            requeue: tx,
        }
    }

    /// Returns a publisher bound to this broker.
    #[must_use]
    pub fn publisher(&self) -> MemoryPublisher {
        MemoryPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl std::fmt::Debug for MemoryBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBroker").finish_non_exhaustive()
    }
}

impl Broker for MemoryBroker {
    type Subscriber = MemorySubscriber;
    type Publisher = MemoryPublisher;
    type Error = Infallible;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.state
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
            .clear();
        Ok(())
    }
}

// `Self::subscribe` would read as a recursive call into this trait method; spell out the broker
// type so it resolves to the inherent constructor (inherent methods win in path syntax anyway).
#[allow(clippy::use_self)]
impl Subscribe for MemoryBroker {
    async fn subscribe(&self, topic: &str) -> Result<Self::Subscriber, Self::Error> {
        Ok(MemoryBroker::subscribe(self, topic))
    }
}

/// Subscriber returned by [`MemoryBroker::subscribe`]. Yields one [`MemoryMessage`] per
/// delivery; consumers must call `ack` or `nack` on each.
pub struct MemorySubscriber {
    topic: String,
    rx: Option<mpsc::UnboundedReceiver<MemoryDelivery>>,
    requeue: Sender,
}

impl std::fmt::Debug for MemorySubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySubscriber")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl Subscriber for MemorySubscriber {
    type Message = MemoryMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let rx = self
            .rx
            .take()
            .expect("MemorySubscriber::stream called more than once");
        let requeue = self.requeue.clone();
        UnboundedReceiverStream::new(rx).map(move |delivery| {
            Ok(MemoryMessage {
                delivery: Some(delivery),
                requeue: requeue.clone(),
            })
        })
    }
}

/// Publisher returned by [`MemoryBroker::publisher`]. Fanout copy to every subscriber of the
/// target topic at publish time.
#[derive(Clone)]
pub struct MemoryPublisher {
    state: Arc<MemoryState>,
}

impl std::fmt::Debug for MemoryPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPublisher").finish_non_exhaustive()
    }
}

impl Publisher for MemoryPublisher {
    type Error = Infallible;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let delivery = MemoryDelivery {
            topic: msg.topic().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        };
        self.state.fanout(&delivery);
        Ok(())
    }
}

impl BatchPublisher for MemoryPublisher {}

/// A delivery yielded by [`MemorySubscriber::stream`].
///
/// Consumers call [`IncomingMessage::ack`] to confirm processing or
/// [`IncomingMessage::nack`] to negatively acknowledge. `nack` with `requeue = true` pushes the
/// delivery back to the same subscriber's queue; with `requeue = false` it is dropped.
pub struct MemoryMessage {
    delivery: Option<MemoryDelivery>,
    requeue: Sender,
}

impl std::fmt::Debug for MemoryMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryMessage")
            .field("topic", &self.delivery.as_ref().map(|d| d.topic.as_str()))
            .finish_non_exhaustive()
    }
}

impl MemoryMessage {
    /// Returns the topic the message was published to.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.delivery
            .as_ref()
            .map(|d| d.topic.as_str())
            .unwrap_or_default()
    }

    /// Converts the delivery into a broker-agnostic [`RawMessage`]. Consumes the handle without
    /// acknowledging; useful only for assertions that do not care about ack state.
    ///
    /// # Panics
    ///
    /// Panics if the delivery has already been moved out (only possible if internal invariants
    /// were violated; not reachable through the public API).
    #[must_use]
    pub fn into_raw(mut self) -> RawMessage {
        let delivery = self.delivery.take().expect("delivery already consumed");
        RawMessage::new(delivery.topic, delivery.payload).with_headers(delivery.headers)
    }
}

impl IncomingMessage for MemoryMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &Headers {
        static EMPTY: OnceLock<Headers> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(Headers::new), |d| &d.headers)
    }

    async fn ack(mut self) -> Result<(), AckError> {
        self.delivery.take();
        Ok(())
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let delivery = self.delivery.take().expect("delivery already consumed");
        if requeue {
            let _ = self.requeue.send(delivery);
        }
        Ok(())
    }
}

impl TestClient for MemoryBroker {
    type Broker = Self;
    type Error = Infallible;

    async fn start() -> Result<Self, Self::Error> {
        Ok(Self::new())
    }

    fn broker(&self) -> &Self::Broker {
        self
    }

    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let publisher = Self::publisher(self);
        publisher
            .publish(OutgoingMessage::new(topic, payload))
            .await
    }

    async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<<Self::Broker as Broker>::Subscriber, Self::Error> {
        Ok(Self::subscribe(self, topic))
    }

    async fn publisher(&self) -> Result<<Self::Broker as Broker>::Publisher, Self::Error> {
        Ok(Self::publisher(self))
    }

    async fn expect_published(
        &self,
        topic: &str,
        count: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<RawMessage>, Self::Error> {
        let topic_for_wait = topic.to_owned();
        let topic_for_fallback = topic_for_wait.clone();
        let state = Arc::clone(&self.state);

        let wait = async move {
            loop {
                {
                    let log = state
                        .published
                        .lock()
                        .expect("memory broker mutex poisoned");
                    if let Some(messages) = log.get(&topic_for_wait) {
                        if messages.len() >= count {
                            return messages.iter().take(count).cloned().collect::<Vec<_>>();
                        }
                    }
                }
                state.notify.notified().await;
            }
        };

        let result = timeout(timeout_duration, wait).await;
        let messages = result.unwrap_or_else(|_| {
            self.state
                .published
                .lock()
                .expect("memory broker mutex poisoned")
                .get(&topic_for_fallback)
                .map(|m| m.iter().take(count).cloned().collect())
                .unwrap_or_default()
        });
        Ok(messages)
    }

    async fn shutdown(self) -> Result<(), Self::Error> {
        <Self as Broker>::shutdown(&self).await
    }
}
