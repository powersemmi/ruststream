//! In-process broker that keeps every message in memory.
//!
//! [`MemoryBroker`] implements [`Broker`] with broadcast semantics: each subscriber receives a
//! copy of every message published to its name after the subscription was opened. There is no
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
    AckError, Broker, Headers, IncomingMessage, OutgoingMessage, Publisher, RawMessage, Subscribe,
    Subscriber, SubscriptionSource, testing::TestClient,
};
use bytes::Bytes;
use futures::Stream;
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};

type Sender = mpsc::UnboundedSender<MemoryDelivery>;

#[derive(Clone)]
struct MemoryDelivery {
    name: String,
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
    fn register(&self, name: String, tx: Sender) {
        let mut subs = self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        subs.entry(name).or_default().push(tx);
    }

    fn fanout(&self, delivery: &MemoryDelivery) {
        let snapshot = RawMessage::new(delivery.name.clone(), delivery.payload.clone())
            .with_headers(delivery.headers.clone());
        {
            let mut log = self.published.lock().expect("memory broker mutex poisoned");
            log.entry(delivery.name.clone()).or_default().push(snapshot);
        }
        self.notify.notify_waiters();

        let subs = self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        if let Some(senders) = subs.get(&delivery.name) {
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

    /// Opens a subscription to `name`. The returned subscriber starts receiving messages
    /// published after this call; messages published earlier are not buffered.
    #[must_use]
    pub fn subscribe(&self, name: impl Into<String>) -> MemorySubscriber {
        let (tx, rx) = mpsc::unbounded_channel();
        let name = name.into();
        self.state.register(name.clone(), tx.clone());
        MemorySubscriber {
            name,
            rx,
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
    type Subscriber = MemorySubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        Ok(MemoryBroker::subscribe(self, name))
    }
}

/// A subscription descriptor for [`MemoryBroker`], naming the subject to receive on.
///
/// The broker-owned counterpart to the generic [`Name`](crate::Name) source: it carries no extra
/// configuration (the in-memory broker has none), but giving every broker its own
/// [`SubscriptionSource`] keeps the macro-subscriber and lazy-startup paths uniform across brokers.
/// Pass it to the descriptor form of the macro, `#[subscriber(MemorySource::new("orders"))]`, the
/// way a NATS service passes `SubscribeOptions`.
#[derive(Debug, Clone)]
pub struct MemorySource {
    name: String,
}

impl MemorySource {
    /// Creates a source bound to `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl SubscriptionSource<MemoryBroker> for MemorySource {
    type Subscriber = MemorySubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(self, broker: &MemoryBroker) -> Result<Self::Subscriber, Infallible> {
        Ok(broker.subscribe(self.name))
    }
}

/// Subscriber returned by [`MemoryBroker::subscribe`]. Yields one [`MemoryMessage`] per
/// delivery; consumers must call `ack` or `nack` on each.
pub struct MemorySubscriber {
    name: String,
    rx: mpsc::UnboundedReceiver<MemoryDelivery>,
    requeue: Sender,
}

impl std::fmt::Debug for MemorySubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySubscriber")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Subscriber for MemorySubscriber {
    type Message = MemoryMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(MemoryMessage {
                        delivery: Some(delivery),
                        requeue: requeue.clone(),
                    })
                })
            })
        })
    }
}

/// Publisher returned by [`MemoryBroker::publisher`]. Fanout copy to every subscriber of the
/// target name at publish time.
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
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        };
        self.state.fanout(&delivery);
        Ok(())
    }
}

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
            .field("name", &self.delivery.as_ref().map(|d| d.name.as_str()))
            .finish_non_exhaustive()
    }
}

impl MemoryMessage {
    /// Returns the name the message was published to.
    #[must_use]
    pub fn name(&self) -> &str {
        self.delivery
            .as_ref()
            .map(|d| d.name.as_str())
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
        RawMessage::new(delivery.name, delivery.payload).with_headers(delivery.headers)
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
    type Subscriber = MemorySubscriber;
    type Publisher = MemoryPublisher;
    type Error = Infallible;

    async fn start() -> Result<Self, Self::Error> {
        Ok(Self::new())
    }

    fn broker(&self) -> &Self::Broker {
        self
    }

    async fn publish(&self, name: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let publisher = Self::publisher(self);
        publisher.publish(OutgoingMessage::new(name, payload)).await
    }

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        Ok(Self::subscribe(self, name))
    }

    async fn publisher(&self) -> Result<Self::Publisher, Self::Error> {
        Ok(Self::publisher(self))
    }

    async fn expect_published(
        &self,
        name: &str,
        count: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<RawMessage>, Self::Error> {
        let name_for_wait = name.to_owned();
        let name_for_fallback = name_for_wait.clone();
        let state = Arc::clone(&self.state);

        let wait = async move {
            loop {
                {
                    let log = state
                        .published
                        .lock()
                        .expect("memory broker mutex poisoned");
                    if let Some(messages) = log.get(&name_for_wait) {
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
                .get(&name_for_fallback)
                .map(|m| m.iter().take(count).cloned().collect())
                .unwrap_or_default()
        });
        Ok(messages)
    }

    async fn shutdown(self) -> Result<(), Self::Error> {
        <Self as Broker>::shutdown(&self).await
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn stream_can_be_reentered() {
        let broker = MemoryBroker::new();
        let mut sub = MemoryBroker::subscribe(&broker, "test");
        let publisher = broker.publisher();

        publisher
            .publish(OutgoingMessage::new("test", b"one".as_slice()))
            .await
            .unwrap();
        {
            let mut stream = std::pin::pin!(sub.stream());
            let msg = stream.next().await.unwrap().unwrap();
            assert_eq!(msg.payload(), b"one");
            msg.ack().await.unwrap();
        }

        // Helpers like `conformance::helpers::next_message` re-enter `stream` per call; the
        // subscriber must keep yielding after the first stream is dropped.
        publisher
            .publish(OutgoingMessage::new("test", b"two".as_slice()))
            .await
            .unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"two");
        msg.ack().await.unwrap();
    }
}
