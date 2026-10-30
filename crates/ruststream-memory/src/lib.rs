//! The in-memory broker of [RustStream](https://docs.rs/ruststream): a full broker that keeps its
//! queues inside the process, and the reference implementation of the broker contract.
//!
//! Services use it through the `ruststream` facade, as
//! [`ruststream::memory`](https://docs.rs/ruststream/latest/ruststream/memory/), whose overview
//! teaches it.

mod broker;
mod capability;
mod log;
mod pattern;
mod subscriber;

pub use broker::{
    ClosedMemoryBroker, ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish,
    MemoryPublisher, MemoryRequest,
};
pub use capability::{
    MemoryBatchContext, MemoryContext, MemoryPosition, MemoryRequester, MemorySeeker,
    MemoryTransaction, PARTITION_KEY_HEADER, Position, RequestError, SeekHandle,
};
use log::LogState;
pub use log::{Discarding, LogMode, Retaining, Retention};
pub use pattern::{MemoryPattern, PatternError, Routing};
use pattern::{Pattern, PatternGroup, PatternReach};
use subscriber::SubscriptionShared;
pub use subscriber::{MemoryMessage, MemorySource, MemorySubscriber};

#[cfg(feature = "testing")]
use std::sync::OnceLock;
use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use ruststream_core::{BytesMut, HeaderMap, OutgoingMessage};
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;
use tokio::sync::{Notify, mpsc};

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
/// The routing rule outlives a shutdown, because it is the broker's setting rather than a
/// registration: a revived bus routes the way the broker was built to.
enum Bus {
    Live(Registry),
    ShutDown(Routing),
}

impl Default for Bus {
    fn default() -> Self {
        Self::Live(Registry::new(Routing::default()))
    }
}

/// The live bus's registrations: the subscriptions of exact names, the pattern subscriptions,
/// and the rule that picks between them.
///
/// An exact name is a map key because it is the name every delivery to that subscription
/// carries: the fanout finds it in the lookup it has to do anyway and stamps the delivery with
/// it, so a publish to a name someone reads allocates nothing for the name. The patterns sit
/// beside the map rather than in it, so a broker with none of them spends one emptiness check on
/// them per publish.
struct Registry {
    names: HashMap<Arc<str>, Vec<Sender>>,
    patterns: Vec<PatternGroup>,
    routing: Routing,
}

impl Registry {
    fn new(routing: Routing) -> Self {
        Self {
            names: HashMap::new(),
            patterns: Vec::new(),
            routing,
        }
    }

    /// How many subscriber registrations the bus holds.
    fn len(&self) -> usize {
        self.names.values().map(Vec::len).sum::<usize>()
            + self
                .patterns
                .iter()
                .map(|group| group.senders.len())
                .sum::<usize>()
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
    /// [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) run. Empty in production, so `fanout` does no extra work.
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

    /// The bus, for a call that answers through its `Result`.
    ///
    /// The bus changes by whole steps (a push, an insert, a swap to shut down), so a lock that a
    /// panic poisoned still guards a whole bus and the call goes on with it.
    fn lock_bus(&self) -> MutexGuard<'_, Bus> {
        self.subscribers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The published log, for a call that answers through its `Result`; poisoning as for
    /// [`lock_bus`](Self::lock_bus).
    fn lock_log(&self) -> MutexGuard<'_, LogState> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
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
        match &mut *self.lock_bus() {
            Bus::Live(registry) => {
                registry.names.entry(Arc::from(name)).or_default().push(tx);
                Ok(())
            }
            Bus::ShutDown(_) => Err(MemoryError::ShutDown),
        }
    }

    /// Registers a pattern subscription's sender on the live bus, joining the group of the same
    /// pattern text if one is registered.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] against a shut-down bus.
    fn register_pattern(
        &self,
        text: &str,
        pattern: Pattern,
        tx: Sender,
    ) -> Result<(), MemoryError> {
        match &mut *self.lock_bus() {
            Bus::Live(registry) => {
                match registry
                    .patterns
                    .iter_mut()
                    .find(|group| &*group.text == text)
                {
                    Some(group) => group.senders.push(tx),
                    None => registry.patterns.push(PatternGroup {
                        text: text.into(),
                        pattern,
                        senders: vec![tx],
                    }),
                }
                Ok(())
            }
            Bus::ShutDown(_) => Err(MemoryError::ShutDown),
        }
    }

    /// Sets the broker's routing rule, on a live bus and on a shut-down one alike.
    fn set_routing(&self, routing: Routing) {
        match &mut *self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
        {
            Bus::Live(registry) => registry.routing = routing,
            Bus::ShutDown(kept) => *kept = routing,
        }
    }

    /// The broker's routing rule, which a shut-down bus keeps for its revival.
    #[cfg(any(test, feature = "testing"))]
    fn routing(&self) -> Routing {
        match &*self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
        {
            Bus::Live(registry) => registry.routing,
            Bus::ShutDown(routing) => *routing,
        }
    }

    // Request inboxes are single-use; dropping the whole entry keeps the subscriber map from
    // accumulating one dead sender per completed request. A shut-down bus has nothing to drop.
    fn unregister(&self, name: &str) {
        if let Bus::Live(registry) = &mut *self
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned")
        {
            registry.names.remove(name);
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
    /// Pattern subscriptions are consulted only where some are registered, and the broker's
    /// [`Routing`] picks which of them take the publish.
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
            let bus = self.lock_bus();
            let Bus::Live(registry) = &*bus else {
                return Err(MemoryError::ShutDown);
            };
            // A discarding broker takes neither the log lock nor a position: nothing can ask
            // for a replay, so the publish is the fanout alone.
            let mut log = self
                .recording
                .load(Ordering::Acquire)
                .then(|| self.lock_log());
            // A broker with pattern subscriptions takes its own path, so the one without them
            // pays a single emptiness check for their existence.
            if registry.patterns.is_empty() {
                let registered = registry.names.get_key_value(name);
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
            } else {
                self.fanout_with_patterns(registry, log.as_deref_mut(), name, payload, headers);
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

    /// The fanout of a broker with pattern subscriptions: the exact name's subscriptions and
    /// the patterns the broker's [`Routing`] picks.
    ///
    /// A publish that reaches a pattern alone allocates the name it carries, since no
    /// registration owns it. Out of line, so the fanout of a broker with no pattern subscription
    /// compiles to what it was before patterns existed.
    #[cold]
    #[inline(never)]
    fn fanout_with_patterns(
        &self,
        registry: &Registry,
        log: Option<&mut LogState>,
        name: &str,
        payload: Bytes,
        headers: HeaderMap,
    ) {
        let registered = registry.names.get_key_value(name);
        // An exact name whose subscriptions have all gone takes nothing, so under
        // `MostSpecific` it must not shadow a pattern that is still listening.
        let exact_live =
            registered.is_some_and(|(_, senders)| senders.iter().any(|sender| !sender.is_closed()));
        let reach = registry.routing.reach(&registry.patterns, name, exact_live);
        let shared = match registered {
            Some((registered, _)) => Arc::clone(registered),
            None if log.is_some() || !matches!(reach, PatternReach::Nobody) => Arc::from(name),
            None => return,
        };
        let shared = Arc::new(DeliveryInner {
            name: shared,
            payload,
            headers,
        });
        let seq = log.map_or(0, |log| log.append(&shared));
        let delivery = MemoryDelivery {
            shared,
            seq,
            deliveries: NonZeroU64::MIN,
        };
        if let Some((_, senders)) = registered {
            self.send_to(senders, &delivery);
        }
        match reach {
            PatternReach::Nobody => {}
            PatternReach::EveryFrom(first) => {
                for group in registry.patterns[first..]
                    .iter()
                    .filter(|group| group.pattern.matches(name))
                {
                    self.send_to_pattern(&group.senders, &delivery);
                }
            }
            PatternReach::Only(index) => {
                self.send_to_pattern(&registry.patterns[index].senders, &delivery);
            }
        }
    }

    /// Enqueues `delivery` to the senders of a pattern subscription.
    ///
    /// Every one of them is a subscription with a dispatch loop behind it, whatever name the
    /// delivery carries, so under the harness every enqueue counts.
    fn send_to_pattern(&self, senders: &[Sender], delivery: &MemoryDelivery) {
        for tx in senders {
            let sent = tx.send(delivery.clone());
            #[cfg(feature = "testing")]
            if sent.is_ok()
                && let Some(coordinator) = self.coordinator.get()
            {
                coordinator.enqueued();
            }
            #[cfg(not(feature = "testing"))]
            let _ = sent;
        }
    }

    /// Installs the harness coordinator for a [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) run, and
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

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{
        Broker, ConnectedBroker, IncomingMessage, Publisher, Subscribe, Subscriber,
        SubscriptionSource,
    };

    use super::*;

    /// What the default broker keeps: nothing. A service publishing forever on it grows only by
    /// what its subscribers have yet to read.
    #[tokio::test]
    async fn a_discarding_broker_records_nothing_it_publishes() {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        for i in 0..100u8 {
            publisher
                .publish(OutgoingMessage::new("orders", &[i]), None)
                .await
                .unwrap();
        }

        assert!(!broker.state.recording.load(Ordering::Acquire));
        assert!(broker.state.log.lock().unwrap().name("orders").is_none());
    }

    /// The bound holds per name: the newest messages stay, the oldest are evicted, and the
    /// positions of what is left do not shift.
    #[tokio::test]
    async fn a_retaining_broker_evicts_past_its_message_bound() {
        let broker = MemoryBroker::retaining(Retention::Messages(ruststream_core::nonzero!(2)));
        let publisher = broker.publisher();
        for i in 0..5u8 {
            publisher
                .publish(OutgoingMessage::new("orders", &[i]), None)
                .await
                .unwrap();
        }

        let log = broker.state.log.lock().unwrap();
        let retained = log.name("orders").expect("the name was published to");
        let first_seq = retained.first_seq();
        let next_seq = retained.next_seq();
        let payloads: Vec<Vec<u8>> = retained
            .messages("orders")
            .iter()
            .map(|msg| msg.payload().to_vec())
            .collect();
        drop(log);

        assert_eq!(first_seq, 3, "the first three are evicted");
        assert_eq!(next_seq, 5, "positions stay absolute");
        assert_eq!(payloads, [vec![3], vec![4]]);
    }

    /// A payload wider than the byte bound is kept alone rather than dropped on arrival: the
    /// message a publish just produced must be readable back.
    #[tokio::test]
    async fn a_byte_bound_keeps_the_newest_message_whatever_its_size() {
        let broker = MemoryBroker::retaining(Retention::Bytes(ruststream_core::nonzero!(4)));
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("frames", &[0u8; 64]), None)
            .await
            .unwrap();

        let retained = {
            let log = broker.state.log.lock().unwrap();
            log.name("frames")
                .expect("the name was published to")
                .messages("frames")
        };
        assert_eq!(retained.len(), 1);
    }

    /// The payload of each name's log, oldest first, as the retention bound left it.
    fn retained(broker: &MemoryBroker<Retaining>, name: &str) -> Vec<Vec<u8>> {
        let log = broker.state.log.lock().unwrap();
        log.name(name)
            .expect("the name was published to")
            .messages(name)
            .iter()
            .map(|msg| msg.payload().to_vec())
            .collect()
    }

    /// A byte bound evicts the oldest messages until what is left fits, whatever their count.
    #[tokio::test]
    async fn a_byte_bound_evicts_the_oldest_past_its_budget() {
        let broker = MemoryBroker::retaining(Retention::Bytes(ruststream_core::nonzero!(4)));
        let publisher = broker.publisher();
        for i in 0..3u8 {
            publisher
                .publish(OutgoingMessage::new("frames", &[i, i]), None)
                .await
                .unwrap();
        }

        assert_eq!(retained(&broker, "frames"), [vec![1, 1], vec![2, 2]]);
    }

    /// Under both bounds at once the first one reached evicts: many small messages meet the count,
    /// a few wide ones meet the bytes.
    #[tokio::test]
    async fn a_combined_bound_evicts_at_whichever_bound_is_reached_first() {
        let broker = MemoryBroker::retaining(Retention::MessagesAndBytes {
            messages: ruststream_core::nonzero!(2),
            bytes: ruststream_core::nonzero!(4),
        });
        let publisher = broker.publisher();
        for i in 0..3u8 {
            publisher
                .publish(OutgoingMessage::new("small", &[i]), None)
                .await
                .unwrap();
        }
        for i in 0..2u8 {
            publisher
                .publish(OutgoingMessage::new("wide", &[i; 3]), None)
                .await
                .unwrap();
        }

        assert_eq!(retained(&broker, "small"), [vec![1], vec![2]]);
        assert_eq!(retained(&broker, "wide"), [vec![1; 3]]);
    }

    /// One name per subscription rather than one per delivery: the fanout stamps the delivery with
    /// the very handle the registration holds, which is what keeps the name off the allocator.
    #[tokio::test]
    async fn a_delivery_carries_the_name_the_registry_holds() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("orders");
        broker
            .publisher()
            .publish(OutgoingMessage::new("orders", b"body"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(subscriber.stream());
        let delivered = stream.next().await.unwrap().unwrap();
        let stamped = &delivered
            .delivery
            .as_ref()
            .expect("the delivery is still held")
            .shared
            .name;

        let bus = broker.state.subscribers.lock().unwrap();
        let Bus::Live(registry) = &*bus else {
            panic!("the bus is live");
        };
        let (registered, _) = registry
            .names
            .get_key_value("orders")
            .expect("the subscription is registered");
        let registered = Arc::clone(registered);
        drop(bus);

        assert!(Arc::ptr_eq(&registered, stamped));
    }

    /// Under `MostSpecific` a subscription that has gone takes nothing: an exact name or a more
    /// specific pattern whose subscribers were all dropped leaves the publish to the next match that
    /// still reads.
    #[tokio::test]
    async fn most_specific_skips_subscriptions_that_have_gone() {
        let connected = MemoryBroker::new()
            .routing(Routing::MostSpecific)
            .connect()
            .await
            .unwrap();
        let exact = connected.subscribe("orders.eu.created").await.unwrap();
        let specific = MemoryPattern::new("orders.eu.*")
            .subscribe(&connected)
            .await
            .unwrap();
        let mut general = MemoryPattern::new("orders.>")
            .subscribe(&connected)
            .await
            .unwrap();
        drop(exact);
        drop(specific);

        connected
            .publisher()
            .publish(OutgoingMessage::new("orders.eu.created", b"body"), None)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(general.stream());
        assert_eq!(
            stream
                .next()
                .now_or_never()
                .flatten()
                .unwrap()
                .unwrap()
                .name(),
            "orders.eu.created"
        );
    }

    /// A pattern subscription reads every name it matches, stamped with the name it was published
    /// to, and a shut-down bus keeps the routing rule for its revival.
    #[tokio::test]
    async fn a_pattern_reads_every_match_and_the_rule_survives_shutdown() {
        let broker = MemoryBroker::new().routing(Routing::MostSpecific);
        let connected = broker.clone().connect().await.unwrap();
        let mut pattern = MemoryPattern::new("orders.>")
            .subscribe(&connected)
            .await
            .unwrap();
        let publisher = connected.publisher();
        for name in ["orders.eu", "orders.eu.created", "invoices.eu"] {
            publisher
                .publish(OutgoingMessage::new(name, b"body"), None)
                .await
                .unwrap();
        }
        {
            let mut stream = std::pin::pin!(pattern.stream());
            for expected in ["orders.eu", "orders.eu.created"] {
                let delivery = stream.next().await.unwrap().unwrap();
                assert_eq!(delivery.name(), expected);
                // Settled, because a delivery dropped unsettled comes back.
                delivery.ack().await.unwrap();
            }
            assert!(stream.next().now_or_never().is_none());
        }

        let closed = connected.shutdown().await.unwrap();
        assert_eq!(closed.subscribers_dropped(), 1);
        let revived = broker.connect().await.unwrap();
        assert_eq!(revived.state.routing(), Routing::MostSpecific);
    }
}
