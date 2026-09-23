//! In-process broker: the bus, and what it keeps of what went over it.
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
//!
//! A [`retry_after`](crate::runtime::HandlerOutcome::retry_after) outcome is this broker's own
//! affair: the delivery returns to the same subscriber once the delay has elapsed, and nothing is
//! republished. Every delivery carries the count of what the broker has handed that subscriber,
//! the first delivery included, and [`MemoryMessage`] reports it through
//! [`IncomingMessage::redelivery_count`]. A registration's `max_attempts(..)` is therefore spent
//! on these redeliveries, and the delivery that spends it goes to the `dead_letter(..)`
//! destination, or is terminated where the registration named none.
//!
//! How much a broker keeps is its own declaration. [`MemoryBroker::new`] keeps nothing: memory
//! does not grow with the message count, and repositioning a subscription does not compile.
//! [`MemoryBroker::retaining`] keeps the newest messages of every name within a [`Retention`]
//! bound, and its subscriptions replay over that log.
//!
//! A service mounting on this broker globs [`prelude`], which carries the core prelude plus this
//! broker's surface and its publish policies under the uniform names a mount site writes.

mod capability;
mod log;
pub mod prelude;

use capability::SeekControl;
pub use capability::{
    MemoryBatchContext, MemoryContext, MemoryPosition, MemoryRequester, MemorySeeker,
    MemoryTransaction, PARTITION_KEY_HEADER, Position, RequestError, SeekHandle,
};
use log::LogState;
pub use log::{Discarding, LogMode, Retaining, Retention};

use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    fmt,
    future::{Future, ready},
    marker::PhantomData,
    num::NonZeroU64,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

#[cfg(feature = "testing")]
use crate::testing::coordinator::Coordinator;
use crate::{
    AckError, AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer,
    FromName, HeaderMap, IncomingMessage, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, RedeliveryAddress, RedeliveryAddressed, ServerSpec, Subscribe, Subscriber,
    SubscriptionSource, Take,
};
use bytes::Bytes;
use futures::Stream;
use thiserror::Error;
use tokio::sync::{Notify, mpsc};
use tokio::time::sleep;

type Sender = mpsc::UnboundedSender<MemoryDelivery>;

/// A message a transaction holds until it settles.
///
/// Distinct from [`MemoryDelivery`] so an unstamped message cannot be enqueued to a
/// subscriber: only [`MemoryState::fanout`] turns a message into a delivery, by assigning
/// its position in the per-name publish log. The name is owned because a buffered publish
/// outlives the call that wrote it; a publish that goes straight to the bus lends its name
/// instead.
#[derive(Clone)]
struct MemoryOutbound {
    name: String,
    payload: Bytes,
    headers: HeaderMap,
}

impl From<OutgoingMessage<'_, BytesMut>> for MemoryOutbound {
    fn from(msg: OutgoingMessage<'_, BytesMut>) -> Self {
        // The publish hands over both the buffer it produced and the map it filled, so the bus
        // keeps them as they stand; the freeze is the one block shared ownership costs.
        let (name, payload, headers) = msg.into_parts();
        Self {
            name: name.to_owned(),
            payload: payload.freeze(),
            headers,
        }
    }
}

/// What every copy of one published message shares: the name it was published to, its payload
/// and its headers.
///
/// One block behind one reference count, rather than a counted field each. Every accessor the
/// consumer contract asks for hands back a borrow ([`IncomingMessage::payload`],
/// [`IncomingMessage::headers`], [`MemoryMessage::name`]), so nothing needs the fields to be
/// counted apart, and a hand-over - the fanout copy per subscriber, a requeue, a replay off the
/// publish log - is one atomic increment instead of three.
struct DeliveryInner {
    name: Arc<str>,
    payload: Bytes,
    headers: HeaderMap,
}

/// A stamped message on its way to subscribers.
///
/// Cloning one is a reference count on the shared block; what sits beside it is per copy rather
/// than per message, which is what a requeue and a replay change.
#[derive(Clone)]
struct MemoryDelivery {
    shared: Arc<DeliveryInner>,
    /// Zero-based index of this message in its name's publish log. Stable across requeues, so
    /// a redelivered message reports the same [`MemoryPosition`].
    seq: usize,
    /// How many times this copy has been handed to its subscriber, counting the delivery it is
    /// about to make. One at fanout, one more on every requeue and every delayed redelivery,
    /// which is what [`IncomingMessage::redelivery_count`] reports and what a registration's
    /// `max_attempts(..)` is spent against.
    ///
    /// Per copy, not per published message: fanout hands every subscriber its own delivery, and
    /// a replay after a seek is a fresh one, so neither inherits another subscription's count.
    deliveries: NonZeroU64,
}

impl MemoryDelivery {
    /// The same delivery on its way back to the subscriber it came from.
    fn redelivered(mut self) -> Self {
        self.deliveries = self.deliveries.saturating_add(1);
        self
    }
}

/// The subscriber bus: alive with its registrations, or terminally shut down.
///
/// One value instead of a map beside a flag, so the lifecycle state and the registrations
/// cannot disagree: every bus operation matches on the variant and reports
/// [`MemoryError::ShutDown`] against a dead bus instead of silently succeeding.
///
/// The key is shared because it is the name every delivery to that subscription carries: the
/// fanout finds it in the lookup it has to do anyway and stamps the delivery with it, so a
/// publish to a name someone reads allocates nothing for the name.
enum Bus {
    Live(HashMap<Arc<str>, Vec<Sender>>),
    ShutDown,
}

impl Default for Bus {
    fn default() -> Self {
        Self::Live(HashMap::new())
    }
}

struct MemoryState {
    subscribers: Mutex<Bus>,
    /// What the broker keeps of what it published, keyed by the shared delivery name so an
    /// append reuses the fanout's `Arc<str>` instead of allocating a fresh key.
    log: Mutex<LogState>,
    /// Whether the log is worth locking. A discarding broker never touches the log mutex on the
    /// publish path; the flag turns on at most once, when a harness run installs its recording.
    recording: AtomicBool,
    notify: Notify,
    inbox_seq: AtomicU64,
    /// The harness's quiescence-and-recording coordinator, installed by a
    /// [`TestApp`](crate::testing::TestApp) run. Empty in production, so `fanout` does no extra work.
    #[cfg(feature = "testing")]
    coordinator: OnceLock<Coordinator>,
}

impl MemoryState {
    /// The state of a broker that keeps no log.
    fn discarding() -> Self {
        Self::with_log(LogState::Discarding, false)
    }

    /// The state of a broker retaining under `retention`.
    fn retaining(retention: Retention) -> Self {
        Self::with_log(LogState::recording(retention), true)
    }

    fn with_log(log: LogState, recording: bool) -> Self {
        Self {
            subscribers: Mutex::new(Bus::default()),
            log: Mutex::new(log),
            recording: AtomicBool::new(recording),
            notify: Notify::new(),
            inbox_seq: AtomicU64::new(0),
            #[cfg(feature = "testing")]
            coordinator: OnceLock::new(),
        }
    }

    /// Registers a subscriber sender on the live bus.
    ///
    /// The name is shared from here on, and a second subscription to the same name joins the
    /// first one's key rather than bringing a copy of the bytes.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] against a shut-down bus; the caller decides whether
    /// that is an error (the `Subscribe` path) or a silent no-registration (the infallible
    /// inherent constructor).
    fn register(&self, name: &str, tx: Sender) -> Result<(), MemoryError> {
        match &mut *self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
        {
            Bus::Live(subscribers) => {
                subscribers.entry(Arc::from(name)).or_default().push(tx);
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
        // What was recorded under the name goes with the registration. A request inbox is used
        // once and never subscribed again, so its reply would otherwise sit in the log, under a
        // name nothing can reach, for the life of the process.
        if self.recording.load(Ordering::Acquire) {
            self.log
                .lock()
                .expect("memory broker mutex poisoned")
                .forget(name);
        }
    }

    /// Stamps a message published to `name` with its log position and fans it out to the live
    /// bus.
    ///
    /// The name arrives as a borrow and is resolved by the lookup the fanout has to do anyway:
    /// the registration under it owns a shared name, and that is the one the delivery carries,
    /// so a publish to a name someone reads allocates nothing for it. A name no subscription
    /// holds is allocated only where the log will keep it, and on a broker that keeps nothing
    /// there is no delivery to build at all.
    ///
    /// Both locks are held across the log append and the sends (subscribers first, then the
    /// log, the order `apply_pending_seek` uses too): a concurrent seek must never observe a
    /// message queued at a subscriber but absent from the log, or the reverse - either would
    /// lose or duplicate the message across a replay.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] against a shut-down bus: nothing is delivered and
    /// nothing is recorded in the published log.
    // significant_drop_tightening misfires here: both guards drop at the end of the minimal
    // block right after their last use.
    #[allow(clippy::significant_drop_tightening)]
    fn fanout(&self, name: &str, payload: Bytes, headers: HeaderMap) -> Result<(), MemoryError> {
        {
            let bus = self
                .subscribers
                .lock()
                .expect("memory broker mutex poisoned");
            let Bus::Live(subscribers) = &*bus else {
                return Err(MemoryError::ShutDown);
            };
            // A discarding broker takes neither the log lock nor a position: nothing can ask
            // for a replay, so the publish is the fanout alone.
            let mut log = self
                .recording
                .load(Ordering::Acquire)
                .then(|| self.log.lock().expect("memory broker mutex poisoned"));
            let registered = subscribers.get_key_value(name);
            let shared = match registered {
                Some((registered, _)) => Some(Arc::clone(registered)),
                // Nobody reads this name. The log still has to name what it keeps, so a
                // recording broker pays for the name there; a broker that keeps nothing has
                // nothing to build a delivery for.
                None => log.is_some().then(|| Arc::from(name)),
            };
            if let Some(name) = shared {
                // The payload and the headers move into the shared block once, and every
                // per-subscriber copy below is one reference count on that block.
                let shared = Arc::new(DeliveryInner {
                    name,
                    payload,
                    headers,
                });
                let seq = log.as_deref_mut().map_or(0, |log| log.append(&shared));
                let delivery = MemoryDelivery {
                    shared,
                    seq,
                    deliveries: NonZeroU64::MIN,
                };
                if let Some((_, senders)) = registered {
                    self.send_to(senders, &delivery);
                }
            }
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// Enqueues `delivery` to the senders registered under its name.
    fn send_to(&self, senders: &[Sender], delivery: &MemoryDelivery) {
        for tx in senders {
            let sent = tx.send(delivery.clone());
            // Count every live enqueue so the harness can drive to quiescence. Request inboxes
            // (`_inbox.`) are excluded: their reply is consumed by the requester, not a dispatch
            // loop, so it carries no coordinator and is never decremented.
            #[cfg(feature = "testing")]
            if sent.is_ok()
                && !delivery.shared.name.starts_with("_inbox.")
                && let Some(coordinator) = self.coordinator.get()
            {
                coordinator.enqueued();
            }
            #[cfg(not(feature = "testing"))]
            let _ = sent;
        }
    }

    /// Installs the harness coordinator for a [`TestApp`](crate::testing::TestApp) run, and
    /// starts recording if the broker keeps no log of its own. Idempotent.
    ///
    /// A discarding broker retains nothing in production, and its own code cannot read a log
    /// back (seeking does not compile without a retaining broker), so recording for the length
    /// of a run gives the published-message assertions something to read without letting a test
    /// observe what production would not. A retaining broker keeps the bound it declared, so its
    /// assertions see exactly what it retains.
    #[cfg(feature = "testing")]
    fn install_coordinator(&self, coordinator: Coordinator) {
        if self.coordinator.set(coordinator).is_ok()
            && self
                .log
                .lock()
                .expect("memory broker mutex poisoned")
                .record_for_harness()
        {
            self.recording.store(true, Ordering::Release);
        }
    }

    /// A clone of the installed coordinator, threaded into each subscriber and delivery so a
    /// requeue can re-count and a consumed delivery can decrement.
    #[cfg(feature = "testing")]
    fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }
}

/// An in-memory reference broker. Cheap to clone.
///
/// The type parameter is the broker's log mode, and it decides what a subscription can do.
/// [`new`](Self::new) builds the default, [`Discarding`] form: nothing is kept, memory does not
/// grow with the message count, and there is no [`Seekable`](crate::Seekable) implementation to
/// reposition a subscription with. [`retaining`](Self::retaining) builds the [`Retaining`] form,
/// which keeps the newest messages of every name within its [`Retention`] bound and replays them
/// on demand.
///
/// # Examples
///
/// ```
/// use ruststream::memory::{MemoryBroker, Retention};
/// use ruststream::nonzero;
///
/// // A service that only routes messages keeps none of them.
/// let bus = MemoryBroker::new();
/// // A service that replays keeps the last 128 messages of every name.
/// let replayable = MemoryBroker::retaining(Retention::Messages(nonzero!(128)));
/// # let _ = (bus, replayable);
/// ```
pub struct MemoryBroker<Log = Discarding> {
    state: Arc<MemoryState>,
    mode: PhantomData<Log>,
}

impl<Log> Clone for MemoryBroker<Log> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            mode: PhantomData,
        }
    }
}

impl Default for MemoryBroker<Discarding> {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBroker<Discarding> {
    /// Creates a new empty broker that keeps no publish log. Equivalent to
    /// [`MemoryBroker::default`].
    ///
    /// Nothing a subscriber has consumed stays in memory, so a long-running service on this
    /// broker holds only what its subscribers have yet to read. Replay is not available: the
    /// [`Seekable`](crate::Seekable) capability is implemented for a
    /// [`retaining`](Self::retaining) broker's subscriptions alone.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(MemoryState::discarding()),
            mode: PhantomData,
        }
    }
}

impl MemoryBroker<Retaining> {
    /// Creates a broker keeping the newest messages of every name within `retention`.
    ///
    /// Its subscriptions are [`Seekable`](crate::Seekable): a seeker replays the retained
    /// messages from any position that has not been evicted. The bound holds per name, so the
    /// broker's footprint is the bound times the number of names it publishes under.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemoryPosition, Retention};
    /// use ruststream::{Broker, OutgoingMessage, Publisher, Seekable, Seeker, Subscriber};
    /// use ruststream::{IncomingMessage, nonzero};
    /// use futures::StreamExt;
    ///
    /// let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
    /// let mut subscriber = broker.subscribe("audit");
    /// let seeker = subscriber.seeker();
    /// broker
    ///     .publisher()
    ///     .publish(OutgoingMessage::new("audit", b"entry"), None)
    ///     .await?;
    ///
    /// seeker.seek(MemoryPosition::start()).await?;
    /// let mut stream = std::pin::pin!(subscriber.stream());
    /// let replayed = stream.next().await.expect("replayed")?;
    /// assert_eq!(replayed.payload(), b"entry");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn retaining(retention: Retention) -> Self {
        Self {
            state: Arc::new(MemoryState::retaining(retention)),
            mode: PhantomData,
        }
    }
}

impl<Log: LogMode> MemoryBroker<Log> {
    /// Opens a subscription to `name`. The returned subscriber starts receiving messages
    /// published after this call; messages published earlier are not delivered, though a
    /// retaining broker's [`Seekable`](crate::Seekable) capability can replay them from its
    /// publish log.
    ///
    /// On a shut-down broker the registration is refused and the subscriber simply never
    /// receives anything, matching this constructor's infallible signature; the
    /// [`Subscribe`] path reports [`MemoryError::ShutDown`] instead.
    #[must_use]
    pub fn subscribe(&self, name: impl Into<String>) -> MemorySubscriber<Log> {
        let (tx, rx) = mpsc::unbounded_channel();
        let name = name.into();
        let _ = self.state.register(&name, tx.clone());
        MemorySubscriber::new(name, rx, tx, &self.state)
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

impl<Log> fmt::Debug for MemoryBroker<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBroker").finish_non_exhaustive()
    }
}

// --8<-- [start:ladder]
impl<Log: LogMode> Broker for MemoryBroker<Log> {
    type Error = MemoryError;
    type Connected = ConnectedMemoryBroker<Log>;

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
        ready(Ok(ConnectedMemoryBroker {
            state: self.state,
            mode: PhantomData,
        }))
    }
}

/// The connected form of [`MemoryBroker`]: the typed witness that [`Broker::connect`] ran.
///
/// Cheap to clone: the in-memory bus is shared state by nature, so the connected form is a
/// shareable handle on it, exactly like the unconnected broker. Subscriptions (the
/// [`Subscribe`] capability, [`MemorySource`]) resolve against this form, and carry over the
/// broker's log mode: only a [`Retaining`] one opens repositionable subscriptions.
pub struct ConnectedMemoryBroker<Log = Discarding> {
    state: Arc<MemoryState>,
    mode: PhantomData<Log>,
}

impl<Log> Clone for ConnectedMemoryBroker<Log> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            mode: PhantomData,
        }
    }
}

impl<Log> fmt::Debug for ConnectedMemoryBroker<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedMemoryBroker")
            .finish_non_exhaustive()
    }
}

impl<Log: LogMode> ConnectedMemoryBroker<Log> {
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

impl<Log: LogMode> ConnectedBroker for ConnectedMemoryBroker<Log> {
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

// --8<-- [end:ladder]

/// The publish policy of the in-memory broker: no options to carry, so it is a unit marker.
///
/// Pairs into a [`MemoryPublisher`] against a [`ConnectedMemoryBroker`], filling the same
/// [`PublishPolicy`] position where a richer broker carries real options (an exchange, a queue
/// timeout, a transactional id).
///
/// It adds nothing to the generated `AsyncAPI` document. The specification's protocol keys are a
/// closed list and `memory` is not one of them, so there is no lawful binding to fill; an `x-`
/// extension would describe a transport that never leaves the process.
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

// --8<-- [start:publish_policy]
impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for MemoryPublish {
    type Live = MemoryPublisher;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl<Log: LogMode> DefaultPublish for ConnectedMemoryBroker<Log> {
    type Policy = MemoryPublish;
}

// --8<-- [end:publish_policy]

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

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for MemoryRequest {
    type Live = MemoryRequester;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker<Log>,
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

impl<Log: LogMode> DescribeServer for MemoryBroker<Log> {
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
impl<Log: LogMode> crate::testing::TestableBroker for ConnectedMemoryBroker<Log> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let (name, payload, headers) = message.into_parts();
        // Injecting into a shut-down bus is a harness bug (both run_suite and TestApp drive
        // the bus strictly before shutdown), so fail loudly instead of losing the message.
        self.state
            .fanout(name, Bytes::copy_from_slice(payload), headers)
            .expect("inject on a shut-down broker: drive the harness before shutdown");
    }

    /// What the broker holds under `name`: everything published there on a discarding broker
    /// (which records for the length of a harness run), and the retained window on a retaining
    /// one, so an assertion never claims more than the broker keeps.
    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state
            .log
            .lock()
            .expect("memory broker mutex poisoned")
            .name(name)
            .map(|log| log.messages(name))
            .unwrap_or_default()
    }
}

// One registration per log mode: the harness recovers a broker by its concrete type, and the
// two modes are two types.
#[cfg(feature = "testing")]
crate::register_testable_broker!(ConnectedMemoryBroker<Discarding>);
#[cfg(feature = "testing")]
crate::register_testable_broker!(ConnectedMemoryBroker<Retaining>);
// --8<-- [end:testable]

// --8<-- [start:subscribe]
impl<Log: LogMode> Subscribe for ConnectedMemoryBroker<Log> {
    type Subscriber = MemorySubscriber<Log>;
    // One subject is both ends of the bus here, so a publish under the name a subscription reads
    // reaches that subscription, and the name is the address.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let name = name.to_owned();
        if let Err(err) = self.state.register(&name, tx.clone()) {
            return ready(Err(err));
        }
        ready(Ok(MemorySubscriber::new(name, rx, tx, &self.state)))
    }
}

// --8<-- [end:subscribe]

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
// --8<-- [start:from_name]
impl FromName for MemorySource {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

// --8<-- [end:from_name]

// --8<-- [start:source]
impl<Log: LogMode> SubscriptionSource<ConnectedMemoryBroker<Log>> for MemorySource {
    type Subscriber = MemorySubscriber<Log>;
    // The bus moves nothing on its own, and one subject is both ends of it, so a copy goes back
    // to the subject the subscription reads.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> Result<Self::Subscriber, MemoryError> {
        Subscribe::subscribe(connected, &self.name).await
    }
}

impl<Log: LogMode> RedeliveryAddressed<ConnectedMemoryBroker<Log>> for MemorySource {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        // One subject is both ends of the bus, and no lookup is needed to say so.
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}
// --8<-- [end:source]

/// Subscriber returned by [`MemoryBroker::subscribe`]. Yields one [`MemoryMessage`] per
/// delivery; consumers must call `ack` or `nack` on each.
///
/// Also consumable in batches through the [`BatchSubscriber`](crate::BatchSubscriber) capability,
/// which caps each batch at the size it is asked for. A subscription of a [`Retaining`] broker is
/// repositionable over its publish log through the [`Seekable`](crate::Seekable) capability: mint
/// a [`MemorySeeker`] with [`seeker`](crate::Seekable::seeker) before opening the stream.
pub struct MemorySubscriber<Log = Discarding> {
    rx: mpsc::UnboundedReceiver<MemoryDelivery>,
    /// What this subscription shares with every delivery it yields and every seeker minted off
    /// it.
    subscription: Arc<SubscriptionShared>,
    mode: PhantomData<Log>,
}

/// One subscription's state that outlives a poll: its name, the bus, the channel a requeue goes
/// back through, and the seek handoff.
///
/// A subscription allocates it once, and every delivery and every [`MemorySeeker`] holds it
/// behind one reference count: a delivery takes one increment where a requeue sender and a
/// seeker would take one each, and a seeker minted off a delivery is that same increment.
struct SubscriptionShared {
    /// The name the subscription reads, which a seek resolves against the publish log.
    name: String,
    /// Bus state, kept so a seek can read the publish log and check liveness.
    state: Arc<MemoryState>,
    /// The subscription's own channel: a requeue and a replay go back through it.
    requeue: Sender,
    /// The pending reposition, the stale-delivery watermark, and the waker that rouses a parked
    /// stream.
    seek: SeekControl,
}

impl<Log> MemorySubscriber<Log> {
    /// The subscriber reading `rx`, whose registration on the bus is `requeue`'s twin.
    fn new(
        name: String,
        rx: mpsc::UnboundedReceiver<MemoryDelivery>,
        requeue: Sender,
        state: &Arc<MemoryState>,
    ) -> Self {
        Self {
            rx,
            subscription: Arc::new(SubscriptionShared {
                name,
                state: Arc::clone(state),
                requeue,
                seek: SeekControl::default(),
            }),
            mode: PhantomData,
        }
    }

    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    ///
    /// Read off the bus at stream time rather than captured at subscribe time: a subscriber built
    /// before the app was handed to the harness would otherwise carry `None` forever and never
    /// decrement what the bus counted in, hanging the quiescence wait.
    #[cfg(feature = "testing")]
    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.subscription.state.coordinator()
    }
}

impl<Log> fmt::Debug for MemorySubscriber<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySubscriber")
            .field("name", &self.subscription.name)
            .finish_non_exhaustive()
    }
}

impl<Log: LogMode> Subscriber for MemorySubscriber<Log> {
    type Message = MemoryMessage<Log>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        #[cfg(feature = "testing")]
        let coordinator = self.coordinator();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            // Register before reading the pending seek: a seek landing between the read and the
            // park then still finds a waker to rouse.
            // Only a seeker wakes this waker, and a discarding subscription mints none.
            if log::retains::<Log>() {
                self.subscription.seek.waker.register(cx.waker());
            }
            self.apply_pending_seek();
            loop {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(delivery)) => {
                        // A stale pre-seek copy (a requeue that raced the seek): drop it, the
                        // replay already covers everything from the watermark on.
                        if log::retains::<Log>()
                            && delivery.seq < self.subscription.seek.watermark()
                        {
                            #[cfg(feature = "testing")]
                            if let Some(coordinator) = &coordinator {
                                coordinator.consumed();
                            }
                            continue;
                        }
                        return Poll::Ready(Some(Ok(MemoryMessage {
                            delivery: Some(delivery),
                            subscription: Some(Arc::clone(&self.subscription)),
                            #[cfg(feature = "testing")]
                            coordinator: coordinator.clone(),
                            mode: PhantomData,
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
    /// A seek named a message the broker no longer keeps: the retention bound evicted it. The
    /// oldest position still replayable is reported, so a caller that wants what is left seeks
    /// to [`MemoryPosition::start`] instead.
    #[error("position {requested} is no longer retained; the log starts at {oldest}")]
    PositionEvicted {
        /// The position the seek asked for.
        requested: usize,
        /// The oldest position the name still retains.
        oldest: usize,
    },
}

impl Publisher for MemoryPublisher {
    // The bus keeps every message it is handed until the last subscriber has read it, so the
    // publish hands the buffer over rather than lending it.
    type Payload = Take;

    type Error = MemoryError;
    // The in-memory bus has no protocol field a message can differ in, so there is nothing to
    // settle per message.
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        {
            let mut txn = self.txn.lock().expect("memory broker mutex poisoned");
            if let Some(buffered) = txn.as_mut() {
                // Buffering is local to this handle and never touches the bus; a commit against
                // a shut-down bus is what reports the error. The name is copied here because the
                // buffer outlives this call.
                buffered.push(MemoryOutbound::from(msg));
                return ready(Ok(()));
            }
        }
        let (name, payload, headers) = msg.into_parts();
        ready(self.state.fanout(name, payload.freeze(), headers))
    }
}

/// A delivery yielded by [`MemorySubscriber::stream`].
///
/// Consumers call [`IncomingMessage::ack`] to confirm processing or
/// [`IncomingMessage::nack`] to negatively acknowledge. `nack` with `requeue = true` pushes the
/// delivery back to the same subscriber's queue; with `requeue = false` it is dropped.
pub struct MemoryMessage<Log = Discarding> {
    delivery: Option<MemoryDelivery>,
    /// The subscription this delivery came from, which a requeue goes back to and the seek
    /// context builds off. `None` for a request-reply inbox message, which has no subscription
    /// behind it: nothing reads the inbox once the request returns, so a requeue drops it.
    subscription: Option<Arc<SubscriptionShared>>,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in flight and
    /// is decremented once when the message is consumed or dropped (see the `Drop` impl). `None`
    /// outside a harness run and for request-reply inbox messages (which are not dispatch-driven).
    #[cfg(feature = "testing")]
    coordinator: Option<Coordinator>,
    mode: PhantomData<Log>,
}

#[cfg(feature = "testing")]
impl<Log> Drop for MemoryMessage<Log> {
    /// Counts this delivery consumed exactly once: on ack, nack, `into_raw`, or an unsettled drop (a
    /// fail-fast panic). A requeue (`nack(true)` / `nack_after`) re-enqueues a fresh delivery first,
    /// so the in-flight count stays balanced across redelivery.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl<Log> fmt::Debug for MemoryMessage<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryMessage")
            .field("name", &self.delivery.as_ref().map(|d| &*d.shared.name))
            .finish_non_exhaustive()
    }
}

impl<Log> MemoryMessage<Log> {
    /// Returns the name the message was published to.
    #[must_use]
    pub fn name(&self) -> &str {
        self.delivery
            .as_ref()
            .map(|d| &*d.shared.name)
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
        // Cold path (test assertions): the name, the payload and the headers are copied out of
        // the shared block here, not on the fanout.
        RawMessage::new(&*delivery.shared.name, delivery.shared.payload.clone())
            .with_headers(delivery.shared.headers.clone())
    }
}

impl<Log: LogMode> IncomingMessage for MemoryMessage<Log> {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.shared.payload.as_ref())
            .unwrap_or_default()
    }

    fn partition_key(&self) -> Option<&[u8]> {
        crate::Partitioned::partition_key(self)
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.shared.headers)
    }

    /// This broker counts what it delivers, so a registration's `max_attempts(..)` is spent on
    /// its own requeues and delayed redeliveries rather than only on the copies the runtime
    /// publishes. The first delivery of a message answers one.
    fn redelivery_count(&self) -> Option<u64> {
        self.delivery.as_ref().map(|d| d.deliveries.get())
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self.delivery.take().expect("delivery already consumed");
        if requeue && let Some(subscription) = &self.subscription {
            let sent = subscription.requeue.send(delivery.redelivered());
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
    /// `delay` has elapsed, not immediately, and counts as one more delivery.
    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("delivery already consumed")
            .redelivered();
        // Taken rather than cloned: the message is consumed here, so its count moves to the
        // redelivery. An inbox reply has no subscription to come back to; see `subscription`.
        let Some(subscription) = self.subscription.take() else {
            return ready(Ok(()));
        };
        // Under the harness, register the redelivery with the coordinator so the in-flight count is
        // re-balanced when it fires and a test can drive it with `TestApp::advance`. The immediate
        // settlement (`NackAfter`) was already recorded; the redelivery is off the synchronous
        // reaction `drive` waits on.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = self.coordinator.clone() {
            let counter = coordinator.clone();
            coordinator.schedule_redelivery(delay, move || {
                if subscription.requeue.send(delivery).is_ok() {
                    counter.enqueued();
                }
            });
            return ready(Ok(()));
        }
        tokio::spawn(async move {
            sleep(delay).await;
            // The subscriber may be gone by then; a dropped receiver is not an error.
            let _ = subscription.requeue.send(delivery);
        });
        ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;
