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
//! Every capability trait has a native implementation here, on the broker's own in-process
//! semantics: request / reply via
//! [`MemoryRequester`], batch consumption on [`MemorySubscriber`], transactions on
//! [`MemoryPublisher`], partition keys on [`MemoryMessage`], and log repositioning through
//! [`MemorySeeker`] over the per-name publish log.

mod capability;

use capability::SeekControl;
pub use capability::{
    MemoryContext, MemoryPosition, MemoryRequester, MemorySeeker, MemoryTransaction,
    PARTITION_KEY_HEADER, Position, RequestError, SeekHandle,
};

use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    fmt,
    future::{Future, ready},
    sync::{Arc, Mutex, OnceLock, atomic::AtomicU64},
    task::Poll,
    time::Duration,
};

#[cfg(feature = "testing")]
use crate::testing::coordinator::Coordinator;
use crate::{
    AckError, Broker, ConnectedBroker, DefaultPublish, DescribeServer, FromName, HeaderMap,
    IncomingMessage, OutgoingMessage, PairError, PublishPolicy, Publisher, RawMessage, ServerSpec,
    Subscribe, Subscriber, SubscriptionSource,
};
use bytes::Bytes;
use futures::Stream;
use thiserror::Error;
use tokio::sync::{Notify, mpsc};
use tokio::time::sleep;

type Sender = mpsc::UnboundedSender<MemoryDelivery>;

/// A message before it reaches the bus: what publishers construct and transactions buffer.
///
/// Distinct from [`MemoryDelivery`] so an unstamped message cannot be enqueued to a
/// subscriber: only [`MemoryState::fanout`] turns an outbound into a delivery, by assigning
/// its position in the per-name publish log.
#[derive(Clone)]
struct MemoryOutbound {
    name: String,
    payload: Bytes,
    headers: HeaderMap,
}

/// A stamped message on its way to subscribers. The name and headers are shared, not owned:
/// fanout clones one delivery per subscriber, so per-subscriber cost is reference-count bumps
/// (plus the [`Bytes`] payload's own cheap clone), not string and map allocations.
#[derive(Clone)]
struct MemoryDelivery {
    name: Arc<str>,
    payload: Bytes,
    headers: Arc<HeaderMap>,
    /// Zero-based index of this message in its name's publish log. Stable across requeues, so
    /// a redelivered message reports the same [`MemoryPosition`].
    seq: usize,
}

/// The subscriber bus: alive with its registrations, or terminally shut down.
///
/// One value instead of a map beside a flag, so the lifecycle state and the registrations
/// cannot disagree: every bus operation matches on the variant and reports
/// [`MemoryError::ShutDown`] against a dead bus instead of silently succeeding.
enum Bus {
    Live(HashMap<String, Vec<Sender>>),
    ShutDown,
}

impl Default for Bus {
    fn default() -> Self {
        Self::Live(HashMap::new())
    }
}

#[derive(Default)]
struct MemoryState {
    subscribers: Mutex<Bus>,
    // Keyed by the shared delivery name, so the per-publish log append reuses the fanout's
    // `Arc<str>` instead of allocating a fresh key (lookups by `&str` work through `Borrow`).
    published: Mutex<HashMap<Arc<str>, Vec<RawMessage>>>,
    notify: Notify,
    inbox_seq: AtomicU64,
    /// The harness's quiescence-and-recording coordinator, installed by a
    /// [`TestApp`](crate::testing::TestApp) run. Empty in production, so `fanout` does no extra work.
    #[cfg(feature = "testing")]
    coordinator: OnceLock<Coordinator>,
}

impl MemoryState {
    /// Registers a subscriber sender on the live bus.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] against a shut-down bus; the caller decides whether
    /// that is an error (the `Subscribe` path) or a silent no-registration (the infallible
    /// inherent constructor).
    fn register(&self, name: String, tx: Sender) -> Result<(), MemoryError> {
        match &mut *self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
        {
            Bus::Live(subscribers) => {
                subscribers.entry(name).or_default().push(tx);
                Ok(())
            }
            Bus::ShutDown => Err(MemoryError::ShutDown),
        }
    }

    // Request inboxes are single-use; dropping the whole entry keeps the subscriber map from
    // accumulating one dead sender per completed request. A shut-down bus has nothing to drop.
    fn unregister(&self, name: &str) {
        if let Bus::Live(subscribers) = &mut *self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
        {
            subscribers.remove(name);
        }
    }

    /// Stamps `outbound` with its log position and fans it out to the live bus.
    ///
    /// Both locks are held across the log append and the sends (subscribers first, then
    /// published, the order `apply_pending_seek` uses too): a concurrent seek must never
    /// observe a message queued at a subscriber but absent from the log, or the reverse -
    /// either would lose or duplicate the message across a replay.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] against a shut-down bus: nothing is delivered and
    /// nothing is recorded in the published log.
    // significant_drop_tightening misfires here: both guards drop at the end of the minimal
    // block right after their last use.
    #[allow(clippy::significant_drop_tightening)]
    fn fanout(&self, outbound: MemoryOutbound) -> Result<(), MemoryError> {
        // The outbound arrives by value: its name and headers move into shared allocations
        // once, and every per-subscriber copy below is reference-count bumps.
        let MemoryOutbound {
            name,
            payload,
            headers,
        } = outbound;
        let name: Arc<str> = name.into();
        let headers = Arc::new(headers);
        {
            let bus = self
                .subscribers
                .lock()
                .expect("memory broker mutex poisoned");
            let Bus::Live(subscribers) = &*bus else {
                return Err(MemoryError::ShutDown);
            };
            let mut log = self.published.lock().expect("memory broker mutex poisoned");
            let entries = log.entry(Arc::clone(&name)).or_default();
            let delivery = MemoryDelivery {
                name: Arc::clone(&name),
                payload: payload.clone(),
                headers: Arc::clone(&headers),
                seq: entries.len(),
            };
            // The log owns its copy.
            entries.push(RawMessage::new(&*name, payload).with_headers((*headers).clone()));
            self.send_to(subscribers, &delivery);
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// Enqueues `delivery` to every subscriber registered under its name.
    fn send_to(&self, subscribers: &HashMap<String, Vec<Sender>>, delivery: &MemoryDelivery) {
        if let Some(senders) = subscribers.get(&*delivery.name) {
            for tx in senders {
                let sent = tx.send(delivery.clone());
                // Count every live enqueue so the harness can drive to quiescence. Request inboxes
                // (`_inbox.`) are excluded: their reply is consumed by the requester, not a dispatch
                // loop, so it carries no coordinator and is never decremented.
                #[cfg(feature = "testing")]
                if sent.is_ok()
                    && !delivery.name.starts_with("_inbox.")
                    && let Some(coordinator) = self.coordinator.get()
                {
                    coordinator.enqueued();
                }
                #[cfg(not(feature = "testing"))]
                let _ = sent;
            }
        }
    }

    /// Installs the harness coordinator for a [`TestApp`](crate::testing::TestApp) run. Idempotent.
    #[cfg(feature = "testing")]
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// A clone of the installed coordinator, threaded into each subscriber and delivery so a
    /// requeue can re-count and a consumed delivery can decrement.
    #[cfg(feature = "testing")]
    fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
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
    /// published after this call; messages published earlier are not delivered by default,
    /// though the [`Seekable`](crate::Seekable) capability can replay them from the publish
    /// log.
    ///
    /// On a shut-down broker the registration is refused and the subscriber simply never
    /// receives anything, matching this constructor's infallible signature; the
    /// [`Subscribe`] path reports [`MemoryError::ShutDown`] instead.
    #[must_use]
    pub fn subscribe(&self, name: impl Into<String>) -> MemorySubscriber {
        let (tx, rx) = mpsc::unbounded_channel();
        let name = name.into();
        let _ = self.state.register(name.clone(), tx.clone());
        MemorySubscriber {
            name,
            rx,
            requeue: tx,
            batch_limit: DEFAULT_BATCH_LIMIT,
            state: Arc::clone(&self.state),
            seek: Arc::new(SeekControl::default()),
            #[cfg(feature = "testing")]
            coordinator: self.state.coordinator(),
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
    /// Unlike [`MemoryBroker::publisher`], which reports [`MemoryError`], a requester awaits a
    /// correlated reply that may never arrive, so its operations report [`RequestError`]: the
    /// shut-down case plus a reply timeout.
    #[must_use]
    pub fn requester(&self) -> MemoryRequester {
        MemoryRequester::new(Arc::clone(&self.state))
    }
}

impl fmt::Debug for MemoryBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBroker").finish_non_exhaustive()
    }
}

impl Broker for MemoryBroker {
    type Error = MemoryError;
    type Connected = ConnectedMemoryBroker;

    /// Connecting is free for an in-process bus. A shut-down bus (a clone lineage may have shut
    /// the shared state down) is revived with a fresh, empty registration map, so the connected
    /// form always starts live; a live bus keeps its registrations.
    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        {
            let mut bus = self
                .state
                .subscribers
                .lock()
                .expect("memory broker mutex poisoned");
            if matches!(*bus, Bus::ShutDown) {
                *bus = Bus::Live(HashMap::new());
            }
        }
        ready(Ok(ConnectedMemoryBroker { state: self.state }))
    }
}

/// The connected form of [`MemoryBroker`]: the typed witness that [`Broker::connect`] ran.
///
/// Cheap to clone: the in-memory bus is shared state by nature, so the connected form is a
/// shareable handle on it, exactly like the unconnected broker. Subscriptions (the
/// [`Subscribe`] capability, [`MemorySource`]) resolve against this form.
#[derive(Clone)]
pub struct ConnectedMemoryBroker {
    state: Arc<MemoryState>,
}

impl fmt::Debug for ConnectedMemoryBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedMemoryBroker")
            .finish_non_exhaustive()
    }
}

impl ConnectedMemoryBroker {
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
    /// See [`MemoryBroker::requester`] for why its operations report [`RequestError`] rather
    /// than [`MemoryError`].
    #[must_use]
    pub fn requester(&self) -> MemoryRequester {
        MemoryRequester::new(Arc::clone(&self.state))
    }
}

impl ConnectedBroker for ConnectedMemoryBroker {
    type Error = MemoryError;
    type Closed = ClosedMemoryBroker;

    /// Enters the terminal shut-down state: the bus itself flips to its `ShutDown` variant, so
    /// every aliased handle that would touch it (a publisher's publish or transaction commit, a
    /// request) errors with [`MemoryError::ShutDown`]. Consuming `self` makes any further use
    /// of this handle a compile error; the returned witness reports how many subscriber
    /// registrations the teardown dropped.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        let dropped = {
            let mut bus = self
                .state
                .subscribers
                .lock()
                .expect("memory broker mutex poisoned");
            match std::mem::replace(&mut *bus, Bus::ShutDown) {
                Bus::Live(subscribers) => subscribers.values().map(Vec::len).sum(),
                Bus::ShutDown => 0,
            }
        };
        ready(Ok(ClosedMemoryBroker {
            subscribers_dropped: dropped,
        }))
    }
}

/// The publish policy of the in-memory broker: no options to carry, so it is a unit marker.
///
/// Pairs into a [`MemoryPublisher`] against a [`ConnectedMemoryBroker`], filling the same
/// [`PublishPolicy`] position where a richer broker carries real options (an exchange, a queue
/// timeout, a transactional id).
///
/// # Examples
///
/// ```
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::{Broker, PublishPolicy};
///
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = MemoryPublish.pair(&connected).await?;
/// # let _ = publisher;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MemoryPublish;

impl PublishPolicy<ConnectedMemoryBroker> for MemoryPublish {
    type Live = MemoryPublisher;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedMemoryBroker {
    type Policy = MemoryPublish;
}

/// The request / reply policy of the in-memory broker; pairs into a [`MemoryRequester`].
///
/// # Examples
///
/// ```
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::{MemoryBroker, MemoryRequest};
/// use ruststream::{Broker, PublishPolicy};
///
/// let connected = MemoryBroker::new().connect().await?;
/// let requester = MemoryRequest.pair(&connected).await?;
/// # let _ = requester;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MemoryRequest;

impl PublishPolicy<ConnectedMemoryBroker> for MemoryRequest {
    type Live = MemoryRequester;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.requester()))
    }
}

/// The terminal witness returned by shutting down a [`ConnectedMemoryBroker`].
///
/// Has no publish or subscribe surface; it carries the teardown diagnostics as plain data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedMemoryBroker {
    subscribers_dropped: usize,
}

impl ClosedMemoryBroker {
    /// How many subscriber registrations were dropped when the bus shut down.
    #[must_use]
    pub fn subscribers_dropped(&self) -> usize {
        self.subscribers_dropped
    }
}

impl DescribeServer for MemoryBroker {
    /// The in-memory broker has no network address, so it describes itself as an in-process server
    /// over the `"memory"` protocol. Registered with
    /// [`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled), the label is its
    /// stable identity, letting a service mount several memory brokers with disjoint routing and
    /// address each one by name.
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("memory")
    }
}

// --8<-- [start:testable]
// The harness drives the connected form: TestApp connects every registered broker before it
// recovers the in-process transport, and run_suite scenarios receive connected brokers.
#[cfg(feature = "testing")]
impl crate::testing::TestableBroker for ConnectedMemoryBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        // Injecting into a shut-down bus is a harness bug (both run_suite and TestApp drive
        // the bus strictly before shutdown), so fail loudly instead of losing the message.
        self.state
            .fanout(MemoryOutbound {
                name: message.name().to_owned(),
                payload: Bytes::copy_from_slice(message.payload()),
                headers: message.headers().clone(),
            })
            .expect("inject on a shut-down broker: drive the harness before shutdown");
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state
            .published
            .lock()
            .expect("memory broker mutex poisoned")
            .get(name)
            .cloned()
            .unwrap_or_default()
    }
}

#[cfg(feature = "testing")]
crate::register_testable_broker!(ConnectedMemoryBroker);
// --8<-- [end:testable]

impl Subscribe for ConnectedMemoryBroker {
    type Subscriber = MemorySubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let name = name.to_owned();
        if let Err(err) = self.state.register(name.clone(), tx.clone()) {
            return ready(Err(err));
        }
        ready(Ok(MemorySubscriber {
            name,
            rx,
            requeue: tx,
            batch_limit: DEFAULT_BATCH_LIMIT,
            state: Arc::clone(&self.state),
            seek: Arc::new(SeekControl::default()),
            #[cfg(feature = "testing")]
            coordinator: self.state.coordinator(),
        }))
    }
}

/// A subscription descriptor for [`MemoryBroker`], naming the subject to receive on.
///
/// The broker-owned counterpart to the generic [`Name`](crate::Name) source: it carries no extra
/// configuration, the in-memory broker having none.
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

// The in-memory subscription needs nothing beyond a name, so it offers the name-only
// constructor the `#[subscriber(MemorySource)]` form builds through.
impl FromName for MemorySource {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for MemorySource {
    type Subscriber = MemorySubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<Self::Subscriber, MemoryError> {
        Subscribe::subscribe(connected, &self.name).await
    }
}

/// Default cap on how many buffered deliveries one batch drains.
const DEFAULT_BATCH_LIMIT: usize = 64;

/// Subscriber returned by [`MemoryBroker::subscribe`]. Yields one [`MemoryMessage`] per
/// delivery; consumers must call `ack` or `nack` on each.
///
/// Also consumable in batches through the
/// [`BatchSubscriber`](crate::BatchSubscriber) capability; see
/// [`set_batch_limit`](Self::set_batch_limit) for the batch size cap. Repositionable over the
/// publish log through the [`Seekable`](crate::Seekable) capability: mint a [`MemorySeeker`]
/// with [`seeker`](crate::Seekable::seeker) before opening the stream.
pub struct MemorySubscriber {
    name: String,
    rx: mpsc::UnboundedReceiver<MemoryDelivery>,
    requeue: Sender,
    batch_limit: usize,
    /// Bus state, kept so a seek can read the publish log and check liveness.
    state: Arc<MemoryState>,
    /// Shared with every [`MemorySeeker`] minted off this subscriber: the pending reposition,
    /// the stale-delivery watermark, and the waker that rouses a parked stream.
    seek: Arc<SeekControl>,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a requeue
    /// re-counts and a consumed delivery decrements. `None` outside a harness run.
    #[cfg(feature = "testing")]
    coordinator: Option<Coordinator>,
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

impl fmt::Debug for MemorySubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        let seeker = Arc::new(crate::Seekable::seeker(self));
        #[cfg(feature = "testing")]
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            // Register before reading the pending seek: a seek landing between the read and the
            // park then still finds a waker to rouse.
            self.seek.waker.register(cx.waker());
            self.apply_pending_seek();
            loop {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(delivery)) => {
                        // A stale pre-seek copy (a requeue that raced the seek): drop it, the
                        // replay already covers everything from the watermark on.
                        if delivery.seq < self.seek.watermark() {
                            #[cfg(feature = "testing")]
                            if let Some(coordinator) = &coordinator {
                                coordinator.consumed();
                            }
                            continue;
                        }
                        return Poll::Ready(Some(Ok(MemoryMessage {
                            delivery: Some(delivery),
                            requeue: requeue.clone(),
                            seek: Some(Arc::clone(&seeker)),
                            #[cfg(feature = "testing")]
                            coordinator: coordinator.clone(),
                        })));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
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
    txn: Mutex<Option<Vec<MemoryOutbound>>>,
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

impl fmt::Debug for MemoryPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPublisher").finish_non_exhaustive()
    }
}

/// Error type of the in-memory broker: returned by [`MemoryPublisher`] and used as the error
/// type of the [`Broker`] / [`ConnectedBroker`] lifecycle and the [`Subscribe`] capability.
///
/// The bus is in-process, so there is no transport to fail: operations against a live bus
/// succeed, and a publish, subscription, or transaction commit through a handle aliasing a
/// shut-down bus reports [`ShutDown`](MemoryError::ShutDown). The transaction variants cover
/// misuse, which the [`TransactionalPublisher`](crate::TransactionalPublisher) contract requires
/// to surface as errors rather than silent no-ops.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MemoryError {
    /// `begin_transaction` was called while a transaction is already open on this handle.
    #[error("a transaction is already open on this publisher handle")]
    TransactionBusy,
    /// `commit` or `abort` was called with no open transaction on this handle.
    #[error("no transaction is open on this publisher handle")]
    NoTransaction,
    /// The operation (a publish, subscribe, transaction commit, or request) ran through a handle
    /// aliasing a bus that was shut down ([`ConnectedBroker::shutdown`]) and not revived by a
    /// sibling clone's [`Broker::connect`].
    #[error("the memory broker is shut down")]
    ShutDown,
}

impl Publisher for MemoryPublisher {
    type Error = MemoryError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        let outbound = MemoryOutbound {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        };
        {
            let mut txn = self.txn.lock().expect("memory broker mutex poisoned");
            if let Some(buffered) = txn.as_mut() {
                // Buffering is local to this handle and never touches the bus; a commit against
                // a shut-down bus is what reports the error.
                buffered.push(outbound);
                return ready(Ok(()));
            }
        }
        ready(self.state.fanout(outbound))
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
    /// The subscription's pre-minted seeker, shared per delivery so the seek context can build
    /// off the message. `None` for a request-reply inbox message, which no dispatch loop and no
    /// seek context ever sees.
    seek: Option<Arc<MemorySeeker>>,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in flight and
    /// is decremented once when the message is consumed or dropped (see the `Drop` impl). `None`
    /// outside a harness run and for request-reply inbox messages (which are not dispatch-driven).
    #[cfg(feature = "testing")]
    coordinator: Option<Coordinator>,
}

#[cfg(feature = "testing")]
impl Drop for MemoryMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, `into_raw`, or an unsettled drop (a
    /// fail-fast panic). A requeue (`nack(true)` / `nack_after`) re-enqueues a fresh delivery first,
    /// so the in-flight count stays balanced across redelivery.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl fmt::Debug for MemoryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryMessage")
            .field("name", &self.delivery.as_ref().map(|d| &*d.name))
            .finish_non_exhaustive()
    }
}

impl MemoryMessage {
    /// Returns the name the message was published to.
    #[must_use]
    pub fn name(&self) -> &str {
        self.delivery.as_ref().map(|d| &*d.name).unwrap_or_default()
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
        // Cold path (test assertions): the shared name and headers are materialized here, not
        // on the fanout.
        RawMessage::new(&*delivery.name, delivery.payload).with_headers((*delivery.headers).clone())
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

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self.delivery.take().expect("delivery already consumed");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses `fanout`, so count the re-enqueue here to balance this message's
            // `Drop` decrement. The redelivered copy is consumed (and decremented) in turn.
            #[cfg(feature = "testing")]
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
            #[cfg(not(feature = "testing"))]
            let _ = sent;
        }
        ready(Ok(()))
    }

    fn supports_nack_after(&self) -> bool {
        true
    }

    /// Native delayed redelivery: the message returns to the same subscriber's queue once
    /// `delay` has elapsed, not immediately.
    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self.delivery.take().expect("delivery already consumed");
        let requeue = self.requeue.clone();
        // Under the harness, register the redelivery with the coordinator so the in-flight count is
        // re-balanced when it fires and a test can drive it with `TestApp::advance`. The immediate
        // settlement (`NackAfter`) was already recorded; the redelivery is off the synchronous
        // reaction `drive` waits on.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = self.coordinator.clone() {
            let counter = coordinator.clone();
            coordinator.schedule_redelivery(delay, move || {
                if requeue.send(delivery).is_ok() {
                    counter.enqueued();
                }
            });
            return ready(Ok(()));
        }
        tokio::spawn(async move {
            sleep(delay).await;
            // The subscriber may be gone by then; a dropped receiver is not an error.
            let _ = requeue.send(delivery);
        });
        ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;
