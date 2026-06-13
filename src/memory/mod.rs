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
//!
//! Every capability trait has a native implementation here, as a first-class feature of the
//! broker's own in-process semantics (not a simulation of someone else's): request / reply via
//! [`MemoryRequester`], batch consumption on [`MemorySubscriber`], transactions on
//! [`MemoryPublisher`], and partition keys on [`MemoryMessage`].

mod capability;

pub use capability::{MemoryRequester, PARTITION_KEY_HEADER, RequestError};

use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicU64},
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
    inbox_seq: AtomicU64,
}

impl MemoryState {
    fn register(&self, name: String, tx: Sender) {
        let mut subs = self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        subs.entry(name).or_default().push(tx);
    }

    // Request inboxes are single-use; dropping the whole entry keeps the subscriber map from
    // accumulating one dead sender per completed request.
    fn unregister(&self, name: &str) {
        let mut subs = self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        subs.remove(name);
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
            batch_limit: DEFAULT_BATCH_LIMIT,
        }
    }

    /// Returns a publisher bound to this broker.
    #[must_use]
    pub fn publisher(&self) -> MemoryPublisher {
        MemoryPublisher {
            state: Arc::clone(&self.state),
            txn: Mutex::new(None),
        }
    }

    /// Returns a request / reply-capable publisher bound to this broker.
    ///
    /// Unlike [`MemoryBroker::publisher`], whose fire-and-forget operations cannot fail, a
    /// requester awaits a correlated reply that may never arrive, so its operations report
    /// [`RequestError`].
    #[must_use]
    pub fn requester(&self) -> MemoryRequester {
        MemoryRequester::new(Arc::clone(&self.state))
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

/// Default cap on how many buffered deliveries one batch drains.
const DEFAULT_BATCH_LIMIT: usize = 64;

/// Subscriber returned by [`MemoryBroker::subscribe`]. Yields one [`MemoryMessage`] per
/// delivery; consumers must call `ack` or `nack` on each.
///
/// Also consumable in batches through the
/// [`BatchSubscriber`](crate::BatchSubscriber) capability; see
/// [`set_batch_limit`](Self::set_batch_limit) for the batch size cap.
pub struct MemorySubscriber {
    name: String,
    rx: mpsc::UnboundedReceiver<MemoryDelivery>,
    requeue: Sender,
    batch_limit: usize,
}

impl MemorySubscriber {
    /// Caps how many buffered deliveries one batch yielded by
    /// [`BatchSubscriber::batches`](crate::BatchSubscriber::batches) may carry (default 64).
    ///
    /// A batch always carries at least one delivery, so a limit of zero behaves like one.
    pub fn set_batch_limit(&mut self, limit: usize) {
        self.batch_limit = limit;
    }
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
///
/// Also implements [`TransactionalPublisher`](crate::TransactionalPublisher): while a
/// transaction is active on this handle, publishes are buffered and fan out together on commit.
pub struct MemoryPublisher {
    state: Arc<MemoryState>,
    // Active transaction buffer of this handle. `None` outside a transaction.
    txn: Mutex<Option<Vec<MemoryDelivery>>>,
}

impl Clone for MemoryPublisher {
    /// A clone is an independent handle on the same broker: it does not join (or carry over)
    /// this handle's active transaction.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            txn: Mutex::new(None),
        }
    }
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
        {
            let mut txn = self.txn.lock().expect("memory broker mutex poisoned");
            if let Some(buffered) = txn.as_mut() {
                buffered.push(delivery);
                return Ok(());
            }
        }
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

    fn partition_key(&self) -> Option<&[u8]> {
        crate::Partitioned::partition_key(self)
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

    /// Native delayed redelivery: the message returns to the same subscriber's queue once
    /// `delay` has elapsed, not immediately.
    async fn nack_after(mut self, delay: Duration) -> Result<(), AckError> {
        let delivery = self.delivery.take().expect("delivery already consumed");
        let requeue = self.requeue.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            // The subscriber may be gone by then; a dropped receiver is not an error.
            let _ = requeue.send(delivery);
        });
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
    async fn debug_formats_and_message_accessors() {
        let broker = MemoryBroker::new();
        assert!(format!("{broker:?}").contains("MemoryBroker"));

        let source = MemorySource::new("orders");
        assert_eq!(source.name(), "orders");

        let publisher = broker.publisher();
        assert!(format!("{publisher:?}").contains("MemoryPublisher"));

        let mut sub = broker.subscribe("dbg");
        assert!(format!("{sub:?}").contains("MemorySubscriber"));

        publisher
            .publish(OutgoingMessage::new("dbg", b"payload".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert!(format!("{msg:?}").contains("MemoryMessage"));
        assert_eq!(msg.name(), "dbg");

        // into_raw consumes the delivery without acking, yielding a broker-agnostic message.
        let raw = msg.into_raw();
        assert_eq!(raw.name(), "dbg");
        assert_eq!(raw.payload(), b"payload");
    }

    // Paused time needs the current-thread runtime; the redelivery timer auto-advances instead
    // of sleeping for real.
    #[tokio::test(start_paused = true)]
    async fn nack_after_redelivers_after_the_delay() {
        let broker = MemoryBroker::new();
        let mut sub = MemoryBroker::subscribe(&broker, "delayed");
        let publisher = broker.publisher();

        publisher
            .publish(OutgoingMessage::new("delayed", b"later".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        msg.nack_after(Duration::from_secs(5)).await.unwrap();

        // Nothing is redelivered while the delay has not elapsed.
        assert!(futures::poll!(stream.next()).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        // The timer task needs a tick to run before the redelivery is visible.
        tokio::task::yield_now().await;

        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.payload(), b"later");
        redelivered.ack().await.unwrap();
    }

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
