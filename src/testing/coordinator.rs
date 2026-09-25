//! The recording-and-quiescence [`Coordinator`] shared by the harness, the dispatch path, and a
//! broker's in-process bus.
//!
//! One `Coordinator` is created per [`TestApp`](super::TestApp) run and installed into both the
//! dispatch [`Delivery`](crate::runtime::dispatch::Delivery) context (via [`TestHooks`]) and each
//! broker's bus. It does two jobs:
//!
//! - Records every delivery a handler saw: the raw payload, headers, and the final outcome
//!   ([`Outcome`]), keyed by the broker's scope id and the subscription name.
//! - Tracks in-flight work so [`TestApp::publish`](super::TestApp) can drive the system to a
//!   standstill before returning: every enqueue into a subscriber increments the counter, every
//!   completed dispatch decrements it.

use std::any::{Any, type_name};
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use crate::runtime::HandlerResult;
use crate::{OutgoingMessage, RawMessage};

use super::TestError;

tokio::task_local! {
    /// The harness driving the current dispatch task, so a slot publisher and the batch settle
    /// path can reach it without threading a handle through every signature they sit behind.
    /// Installed by `dispatch` and `run_batch` around each handler invocation when a harness is
    /// attached.
    static HARNESS: HarnessScope;

    /// Set around one pairing the runtime makes: the connected broker the policy pairs against.
    /// The scope's broker unless a `Bound` token, which pairs against its own broker, says
    /// otherwise.
    static PAIRING: Cell<Origin>;

    /// Set around one publish through a publisher the runtime paired: the broker it was paired
    /// against, which is the broker the message goes to.
    static PUBLISHING: Origin;
}

/// The broker a publisher the runtime paired publishes to: the connected broker it was paired
/// against, whatever scope holds it. A handler on one broker holding a `Bound` token for another
/// publishes to the other, and its publishes are that broker's.
///
/// Known by the connected form's address, which the harness maps to the broker's registration.
/// A publisher the runtime did not pair (one the service builds or keeps itself) carries none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Origin(Option<usize>);

impl Origin {
    /// The connected broker at `connected`.
    fn of<C: ?Sized>(connected: &C) -> Self {
        Self(Some(std::ptr::from_ref(connected).cast::<()>() as usize))
    }
}

/// Runs the pairing `pairing` against `connected`, and reports the broker the paired publisher
/// publishes to.
pub(crate) fn paired<C: ?Sized, F: Future>(
    connected: &C,
    pairing: F,
) -> impl Future<Output = (F::Output, Origin)> {
    PAIRING.scope(Cell::new(Origin::of(connected)), async move {
        let live = pairing.await;
        (live, PAIRING.with(Cell::get))
    })
}

/// Notes, inside a pairing, that the policy pairs against `connected` instead of the broker it
/// was handed: what a `Bound` token does.
pub(crate) fn pairs_against<C: ?Sized>(connected: &C) {
    let _ = PAIRING.try_with(|origin| origin.set(Origin::of(connected)));
}

/// Runs `publish` as a publish to the broker `origin` names.
pub(crate) fn publishing_to<F: Future>(
    origin: Origin,
    publish: F,
) -> impl Future<Output = F::Output> {
    PUBLISHING.scope(origin, publish)
}

/// The broker the publish running in this task goes to, where the runtime paired its publisher.
fn publishing() -> Origin {
    PUBLISHING.try_with(|origin| *origin).unwrap_or_default()
}

/// The harness a dispatch task runs under: which coordinator records it, which broker's
/// registration the delivery belongs to, and which subscription of the app it came through.
#[derive(Clone)]
pub(crate) struct HarnessScope {
    coordinator: Coordinator,
    scope_id: usize,
    subscription: usize,
}

impl HarnessScope {
    pub(crate) const fn new(
        coordinator: Coordinator,
        scope_id: usize,
        subscription: usize,
    ) -> Self {
        Self {
            coordinator,
            scope_id,
            subscription,
        }
    }
}

/// Records a publish made through an `Out` slot, with the broker's per-message options it
/// carried, against the harness driving the current dispatch task, if any. Called by the slot
/// publisher wrapper on every publish; outside a harness-driven handler (production, or a test
/// without a `TestApp`) it is a no-op.
pub(crate) fn record_slot_publish<Options, Payload>(
    slot: &'static str,
    msg: &OutgoingMessage<'_, Payload>,
    options: Option<&Options>,
) where
    Options: Clone + Send + Sync + 'static,
    Payload: AsRef<[u8]>,
{
    let _ = HARNESS.try_with(|scope| {
        scope
            .coordinator
            .record_slot(slot, msg, RecordedOptions::capture(options));
    });
}

/// Records a reply publish's per-message options against the harness driving the current dispatch
/// task, if any. Called by the reply sink for every reply it sends; outside a harness-driven
/// handler it is a no-op.
///
/// Only the options are kept: the reply's message reaches the broker's publish log on its own,
/// and that log is what the channel assertions read.
pub(crate) fn record_reply_publish<Options>(name: &str, options: Option<&Options>)
where
    Options: Clone + Send + Sync + 'static,
{
    let _ = HARNESS.try_with(|scope| {
        scope
            .coordinator
            .record_reply(name, RecordedOptions::capture(options));
    });
}

/// One message the app is handing a broker from a harness-driven task, captured where its publish
/// pipeline hands it over, so the record holds what the broker is handed with every publish layer
/// and transform applied. It enters the harness's record once the broker took it: a publish the
/// broker refused was not published.
///
/// Outside a harness-driven task nothing is captured, and the calls cost a task-local lookup.
/// The record is what a live test's `published` assertions read, and what a live settle waits for
/// the subscriptions to handle.
#[must_use]
pub(crate) struct PipelinePublish(Option<(Coordinator, Origin, RawMessage)>);

impl PipelinePublish {
    /// Captures `msg` for the harness driving the current task, if any.
    pub(crate) fn capture<Payload: AsRef<[u8]>>(msg: &OutgoingMessage<'_, Payload>) -> Self {
        Self(
            HARNESS
                .try_with(|scope| (scope.coordinator.clone(), publishing(), raw_of(msg)))
                .ok(),
        )
    }

    /// Records the captured message: the broker took it.
    pub(crate) fn sent(self) {
        if let Some((coordinator, origin, message)) = self.0 {
            coordinator.record_sent(origin, message);
        }
    }
}

/// The owned copy of `msg` the harness keeps.
fn raw_of<Payload: AsRef<[u8]>>(msg: &OutgoingMessage<'_, Payload>) -> RawMessage {
    RawMessage::new(msg.name().to_owned(), msg.payload().as_ref().to_vec())
        .with_headers(msg.headers().clone())
}

/// The broker's per-message options one slot publish carried, copied and type-erased so the
/// assertions can hand them back to a test as the broker's own type.
///
/// The slot wrapper is the one place the options are still typed: the broker's `publish` resolves
/// them into whatever its protocol does with them, so the publish log underneath sees none.
#[derive(Clone)]
pub(crate) struct RecordedOptions {
    /// The broker's options type, so a `with_options` naming another one fails by name.
    pub(crate) type_name: &'static str,
    /// `None` when the publish carried no options: no step ran, the policy's defaults applied.
    pub(crate) value: Option<Arc<dyn Any + Send + Sync>>,
}

impl RecordedOptions {
    fn capture<Options: Clone + Send + Sync + 'static>(options: Option<&Options>) -> Self {
        Self {
            type_name: type_name::<Options>(),
            value: options.map(|options| Arc::new(options.clone()) as Arc<dyn Any + Send + Sync>),
        }
    }

    /// The recorded options as `Options`, or `None` when the publish carried none.
    ///
    /// # Panics
    ///
    /// Panics when the publish carried options of another type: the test named the wrong
    /// broker's type, and the message says which one was recorded.
    pub(crate) fn downcast<Options: 'static>(&self, channel: &str) -> Option<&Options> {
        let value = self.value.as_ref()?;
        Some(value.as_ref().downcast_ref::<Options>().unwrap_or_else(|| {
            panic!(
                "channel {channel:?} published with per-message options of type `{}`, not `{}`",
                self.type_name,
                type_name::<Options>(),
            )
        }))
    }
}

impl fmt::Debug for RecordedOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordedOptions")
            .field("type_name", &self.type_name)
            .field("set", &self.value.is_some())
            .finish()
    }
}

/// Records one batch against the harness driving the current dispatch task, if any: the batch
/// settle path knows the deliveries and their settlements but not which harness (if any) is
/// watching, and it sits behind four call sites whose signatures would otherwise all have to
/// carry one.
pub(crate) fn record_batch(name: &str, deliveries: Vec<Delivered>) {
    let _ = HARNESS.try_with(|scope| {
        scope.coordinator.record(Record {
            scope_id: scope.scope_id,
            subscription: scope.subscription,
            name: name.to_owned(),
            deliveries,
            panicked: false,
            decode_failed: false,
        });
    });
}

/// One batch's record still owed to the harness, held from the moment the settle path captures
/// the batch until the record is pushed.
///
/// The settlements a batch applies are what release the in-flight count, and the record is
/// written after the last of them, so without this a [`drive`](Coordinator::drive) could return
/// in between and a test would assert against a batch that has not been recorded yet.
pub(crate) struct PendingRecord(Option<Coordinator>);

impl PendingRecord {
    /// Takes the count against the harness driving the current dispatch task, if any.
    pub(crate) fn new() -> Self {
        Self(
            HARNESS
                .try_with(|scope| {
                    scope.coordinator.enqueued();
                    scope.coordinator.clone()
                })
                .ok(),
        )
    }
}

impl Drop for PendingRecord {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.0 {
            coordinator.consumed();
        }
    }
}

/// Runs `fut` with the harness (when one is attached) visible to the recorders above.
pub(crate) async fn in_harness_scope<F: Future>(scope: Option<HarnessScope>, fut: F) -> F::Output {
    match scope {
        Some(scope) => HARNESS.scope(scope, fut).await,
        None => fut.await,
    }
}

/// One publish made through an `Out` slot: the slot's name, the outgoing message captured as a
/// [`RawMessage`] (destination, payload, headers), and the per-message options it carried.
#[derive(Clone, Debug)]
pub(crate) struct SlotRecord {
    pub(crate) slot: &'static str,
    pub(crate) message: RawMessage,
    pub(crate) options: RecordedOptions,
}

/// The classified outcome the harness records for one delivery to a handler.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum Outcome {
    /// The handler acknowledged the message.
    Ack,
    /// The handler nacked with requeue (the broker would redeliver).
    Nack,
    /// The handler dropped the message (nack without requeue).
    Drop,
    /// The payload failed to decode into the handler's input type.
    DecodeFailed,
    /// The handler panicked (under the panic failure policy in effect).
    Panicked,
}

/// One delivery inside a recorded handler call: the payload the handler saw, and how the
/// dispatcher settled it (`None` when a fail-fast panic left the message unsettled).
pub(crate) struct Delivered {
    pub(crate) raw: Bytes,
    pub(crate) settle: Option<HandlerResult>,
}

/// One recorded call into a handler: a single delivery, or a whole batch.
pub(crate) struct Record {
    /// The broker's registration index in the app, used to scope assertions per broker.
    pub(crate) scope_id: usize,
    /// The subscription of the app the call came through, as [`TestHooks::subscribed`] numbered
    /// it: what tells apart two subscriptions that report one name.
    pub(crate) subscription: usize,
    /// The subscription (channel) name the message arrived on.
    pub(crate) name: String,
    /// What this call carried: exactly one delivery for a single-message handler, one per
    /// element of the batch for a batch handler.
    pub(crate) deliveries: Vec<Delivered>,
    /// Whether the handler panicked.
    pub(crate) panicked: bool,
    /// Whether the payload failed to decode before the handler ran.
    pub(crate) decode_failed: bool,
}

impl Record {
    /// Classifies this call into a single [`Outcome`]. Panic and decode-failure dominate the
    /// settlement (a fail-fast panic acks nothing; a skip-policy panic still records `Panicked`).
    ///
    /// A batch settling its elements differently has no one outcome; the first element's stands
    /// for the call, which is why the per-element assertions
    /// ([`settled`](super::SubscriberAssertions::settled)) are what a mixed batch is read with.
    pub(crate) fn outcome(&self) -> Outcome {
        if self.panicked {
            Outcome::Panicked
        } else if self.decode_failed {
            Outcome::DecodeFailed
        } else {
            settled_as(self.deliveries.first().and_then(|one| one.settle))
        }
    }
}

/// The outcome one settlement classifies as. An unsettled delivery (a fail-fast panic tore the
/// service down before it settled) reads as dropped: nothing acknowledged it.
fn settled_as(settle: Option<HandlerResult>) -> Outcome {
    match settle {
        Some(HandlerResult::Ack) => Outcome::Ack,
        Some(HandlerResult::Nack { requeue: true } | HandlerResult::NackAfter { .. }) => {
            Outcome::Nack
        }
        Some(HandlerResult::Nack { requeue: false }) | None => Outcome::Drop,
    }
}

/// A shared slot installed once per broker scope into the dispatch [`Delivery`].
///
/// It is empty in production (the `testing` feature can be on without a harness running), so the
/// per-delivery read is a single atomic load returning `None`. The harness fills it before any
/// dispatch task starts, so the read path never races the write.
pub(crate) struct TestHooks {
    coordinator: OnceLock<Coordinator>,
    /// Every subscription the app mounts, as `(broker scope, subscription name)` in mount order,
    /// noted while the app is built; a subscription's position here is its identity. A live settle
    /// waits only for what reaches one of these: a publish to a name nothing in the app consumes
    /// has nothing to be handled by.
    subscriptions: Mutex<Vec<(usize, String)>>,
}

impl TestHooks {
    /// A hooks slot that is never installed: the production / no-harness path.
    pub(crate) fn detached() -> Self {
        Self {
            coordinator: OnceLock::new(),
            subscriptions: Mutex::new(Vec::new()),
        }
    }

    /// Notes that the broker registered at `scope_id` carries a subscription named `name`, and
    /// returns the subscription's identity. Two subscriptions may report one name (two
    /// subscriptions on one Pulsar topic both report the topic), so the name does not identify
    /// one.
    pub(crate) fn subscribed(&self, scope_id: usize, name: &str) -> usize {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("test hooks subscriptions mutex poisoned");
        subscriptions.push((scope_id, name.to_owned()));
        subscriptions.len() - 1
    }

    /// Every subscription the app mounts, in mount order.
    pub(crate) fn subscriptions(&self) -> Vec<LiveSubscription> {
        self.subscriptions
            .lock()
            .expect("test hooks subscriptions mutex poisoned")
            .iter()
            .enumerate()
            .map(|(id, (scope_id, name))| LiveSubscription {
                id,
                scope_id: *scope_id,
                name: name.clone(),
            })
            .collect()
    }

    /// Installs the coordinator for a harness run. Idempotent; a second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// The installed coordinator, or `None` when no harness is driving this app.
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }
}

/// Records deliveries and tracks in-flight work so the harness can drive a service to quiescence.
///
/// A broker crate receives a `Coordinator` through
/// [`TestableBroker::install_coordinator`](super::TestableBroker) and calls
/// [`enqueued`](Self::enqueued) on every live enqueue into a subscriber and
/// [`consumed`](Self::consumed) when a delivery is acked, nacked, or dropped, so the harness can
/// tell when the in-process reaction has settled.
///
/// Cloning shares the same counters, notifier, and record log (it is an [`Arc`](std::sync::Arc)
/// inside), so the same `Coordinator` can be installed into every broker bus and every dispatch
/// scope at once.
#[derive(Clone)]
pub struct Coordinator {
    inner: Arc<Inner>,
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("in_flight", &self.inner.in_flight.load(Ordering::SeqCst))
            .field("processed", &self.inner.processed.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

struct Inner {
    in_flight: AtomicUsize,
    processed: AtomicUsize,
    max_steps: usize,
    notify: tokio::sync::Notify,
    records: Mutex<Vec<Record>>,
    slot_records: Mutex<Vec<SlotRecord>>,
    /// One entry per reply the runtime published, keyed by the channel it went to.
    reply_records: Mutex<Vec<(String, RecordedOptions)>>,
    /// Every publish the harness saw, in publish order: the test's own, and every message the
    /// app's publish pipeline handed a broker from a handler's dispatch.
    published: Mutex<Vec<Published>>,
    /// Each connected broker's address, with the registration it belongs to: what a paired
    /// publisher's [`Origin`] resolves against.
    brokers: Mutex<Vec<(usize, usize)>>,
    /// The redeliveries a broker took on to make later (a native `nack_after`), for a live settle
    /// to wait for once they fall due.
    redeliveries: Mutex<Vec<DueRedelivery>>,
    timers: Mutex<Vec<Timer>>,
}

/// One publish the harness saw, and the broker it went to: the one the test named, or the one
/// the publisher was paired against.
struct Published {
    scope_id: usize,
    message: RawMessage,
}

/// A subscription a live settle waits on: its identity, the broker it belongs to, and its name.
#[derive(Clone, Debug)]
pub(crate) struct LiveSubscription {
    pub(crate) id: usize,
    pub(crate) scope_id: usize,
    pub(crate) name: String,
}

/// How each broker of the app routes a destination to its subscriptions: the broker's
/// registration, the destination, and the names subscribed on that broker, answered with the
/// positions of those the broker delivers to
/// ([`TestableBroker::routes`](super::TestableBroker::routes)).
pub(crate) type Routing<'a> = &'a (dyn Fn(usize, &str, &[&str]) -> Vec<usize> + Sync);

/// A subscription a live settle is still waiting on.
struct Unsettled {
    subscription: String,
    handled: usize,
    expected: usize,
}

/// A redelivery a broker accepted to make at `due`, to the subscription of the app numbered
/// `subscription`.
struct DueRedelivery {
    subscription: usize,
    due: tokio::time::Instant,
}

/// A scheduled delayed redelivery (`nack_after` / `retry_after`): its deadline and the task that
/// fires it. The harness awaits the due ones when a test advances time.
struct Timer {
    deadline: tokio::time::Instant,
    handle: tokio::task::JoinHandle<()>,
}

/// An open slot in the in-flight count, released on drop; see [`Coordinator::hold`].
pub(crate) struct InFlightHold {
    coordinator: Coordinator,
}

impl Drop for InFlightHold {
    fn drop(&mut self) {
        let inner = &self.coordinator.inner;
        inner.in_flight.fetch_sub(1, Ordering::SeqCst);
        inner.notify.notify_waiters();
    }
}

impl Coordinator {
    /// Creates a coordinator that gives up after `max_steps` dispatched deliveries without
    /// reaching quiescence (a guard against perpetual-requeue handlers).
    pub(crate) fn new(max_steps: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                in_flight: AtomicUsize::new(0),
                processed: AtomicUsize::new(0),
                max_steps,
                notify: tokio::sync::Notify::new(),
                records: Mutex::new(Vec::new()),
                slot_records: Mutex::new(Vec::new()),
                reply_records: Mutex::new(Vec::new()),
                published: Mutex::new(Vec::new()),
                brokers: Mutex::new(Vec::new()),
                redeliveries: Mutex::new(Vec::new()),
                timers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Marks one message enqueued into a subscriber. Called by a broker on every live enqueue into a
    /// delivery channel (initial fanout and every requeue), so the redelivery cycle stays balanced.
    pub fn enqueued(&self) {
        self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Marks one in-flight delivery consumed: acked, nacked, or dropped (a fail-fast panic). A
    /// broker calls this once per delivery (typically from its message's `Drop`), so every delivery
    /// is balanced exactly once regardless of the dispatch path (single, batch, or panic). A requeue
    /// re-enqueues separately, so the cycle stays balanced.
    pub fn consumed(&self) {
        self.inner.processed.fetch_add(1, Ordering::SeqCst);
        self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Keeps the reaction open until the returned guard drops, without counting a delivery.
    ///
    /// The runtime's immediate retry copy settles the original before it publishes the copy, and
    /// the copy's enqueue is what counts it; between the two the in-flight count would read zero
    /// and [`drive`](Self::drive) would return before the copy arrived.
    pub(crate) fn hold(&self) -> InFlightHold {
        self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        InFlightHold {
            coordinator: self.clone(),
        }
    }

    /// Records what a handler saw and how it settled. Called from `dispatch` before the message is
    /// settled, so the record is visible by the time the matching [`consumed`](Self::consumed)
    /// wakes [`drive`](Self::drive).
    pub(crate) fn record(&self, record: Record) {
        self.inner
            .records
            .lock()
            .expect("coordinator records mutex poisoned")
            .push(record);
    }

    /// Schedules a delayed redelivery (`nack_after` / `retry_after`): after `delay`, `redeliver`
    /// runs (it must re-enqueue the message and call [`enqueued`](Self::enqueued)). The redelivery is
    /// off the synchronous reaction the harness drives, so a publish returns once the immediate
    /// settlement is recorded; a test advances time with [`TestApp::advance`](super::TestApp) to
    /// fire it.
    ///
    /// A broker calls this from its `nack_after` instead of a bare `tokio::spawn`, so the harness can
    /// await the fired timers deterministically under a paused clock.
    ///
    /// # Panics
    ///
    /// Panics if the internal timers mutex was poisoned by an earlier panic while it was held.
    pub fn schedule_redelivery<F>(&self, delay: Duration, redeliver: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let deadline = tokio::time::Instant::now() + delay;
        let handle = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            redeliver();
        });
        self.push_timer(deadline, handle);
    }

    /// The awaitable form of [`schedule_redelivery`](Self::schedule_redelivery), for a redelivery
    /// that has to await something itself: the runtime's deferred `retry_after` fallback
    /// re-publishes the message, and the publish is what re-enqueues it. Awaiting the whole
    /// future inside the timer is what makes the copy counted by the time
    /// [`TestApp::advance`](super::TestApp) drives the reaction.
    ///
    /// # Panics
    ///
    /// Panics if the internal timers mutex was poisoned by an earlier panic while it was held.
    pub(crate) fn schedule_redelivery_future<Fut>(&self, delay: Duration, redeliver: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let deadline = tokio::time::Instant::now() + delay;
        let handle = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            redeliver.await;
        });
        self.push_timer(deadline, handle);
    }

    /// Records one scheduled redelivery so [`fire_due_timers`](Self::fire_due_timers) can await it
    /// once its deadline passes.
    fn push_timer(&self, deadline: tokio::time::Instant, handle: tokio::task::JoinHandle<()>) {
        self.inner
            .timers
            .lock()
            .expect("coordinator timers mutex poisoned")
            .push(Timer { deadline, handle });
    }

    /// Awaits every scheduled redelivery whose deadline has now passed, so their re-enqueues are
    /// counted before the caller drives the reaction. Called by `TestApp::advance` after advancing
    /// the clock; redeliveries still in the future stay pending for a later advance.
    // The guard is dropped at the end of the block (before the awaits); held only to drain the due
    // timers out of the shared list.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn fire_due_timers(&self) {
        let now = tokio::time::Instant::now();
        let due: Vec<tokio::task::JoinHandle<()>> = {
            let mut timers = self
                .inner
                .timers
                .lock()
                .expect("coordinator timers mutex poisoned");
            let mut due = Vec::new();
            let mut i = 0;
            while i < timers.len() {
                if timers[i].deadline <= now {
                    due.push(timers.swap_remove(i).handle);
                } else {
                    i += 1;
                }
            }
            due
        };
        for handle in due {
            // The sleep has already elapsed, so the task runs its send and returns; a panic in the
            // (panic-free) timer task is not expected, so a join error is ignored.
            let _ = handle.await;
        }
    }

    /// Waits until no message is in flight, or fails once `max_steps` deliveries have been
    /// dispatched without settling (a non-converging reaction).
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] when the step budget is exhausted before the reaction
    /// settles.
    pub(crate) async fn drive(&self) -> Result<(), TestError> {
        loop {
            // Register interest before reading the counter so a concurrent `settled` cannot slip a
            // wakeup between the check and the await.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.inner.in_flight.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            if self.inner.processed.load(Ordering::SeqCst) >= self.inner.max_steps {
                return Err(TestError::NotQuiescent {
                    processed: self.inner.processed.load(Ordering::SeqCst),
                });
            }
            notified.await;
        }
    }

    /// Notes where each broker of the app is connected, `connected` giving each registration's
    /// connected form: what a paired publisher's [`Origin`] resolves to.
    pub(crate) fn locate<'a>(
        &self,
        connected: impl IntoIterator<Item = &'a (dyn Any + Send + Sync)>,
    ) {
        *self
            .inner
            .brokers
            .lock()
            .expect("coordinator brokers mutex poisoned") = connected
            .into_iter()
            .enumerate()
            .filter_map(|(scope_id, connected)| {
                Origin::of::<dyn Any + Send + Sync>(connected)
                    .0
                    .map(|address| (address, scope_id))
            })
            .collect();
    }

    /// Records one publish the test made onto the broker registered at `scope_id`.
    pub(crate) fn record_published(&self, scope_id: usize, message: RawMessage) {
        self.inner
            .published
            .lock()
            .expect("coordinator published mutex poisoned")
            .push(Published { scope_id, message });
        self.inner.notify.notify_waiters();
    }

    /// Records one publish the app handed a broker, on the broker its publisher was paired
    /// against. A publisher the runtime did not pair names no broker, and its publish is left out:
    /// the harness neither waits for it nor lists it.
    fn record_sent(&self, origin: Origin, message: RawMessage) {
        let scope_id = origin.0.and_then(|address| {
            self.inner
                .brokers
                .lock()
                .expect("coordinator brokers mutex poisoned")
                .iter()
                .find(|(known, _)| *known == address)
                .map(|(_, scope_id)| *scope_id)
        });
        if let Some(scope_id) = scope_id {
            self.record_published(scope_id, message);
        }
    }

    /// Every publish to `name` the harness saw go to the broker registered at `scope_id`, in
    /// publish order.
    pub(crate) fn published(&self, scope_id: usize, name: &str) -> Vec<RawMessage> {
        self.inner
            .published
            .lock()
            .expect("coordinator published mutex poisoned")
            .iter()
            .filter(|published| published.scope_id == scope_id && published.message.name() == name)
            .map(|published| published.message.clone())
            .collect()
    }

    /// Notes that a broker accepted to redeliver a delivery of the subscription numbered
    /// `subscription` after `delay` on its own timer.
    pub(crate) fn expect_redelivery(&self, subscription: usize, delay: Duration) {
        self.inner
            .redeliveries
            .lock()
            .expect("coordinator redeliveries mutex poisoned")
            .push(DueRedelivery {
                subscription,
                due: tokio::time::Instant::now() + delay,
            });
        self.inner.notify.notify_waiters();
    }

    /// The first subscription still owed a delivery, with what it has handled and what it is owed:
    /// every publish its broker delivers to it, as that broker's routing says, and every
    /// redelivery due to it by now.
    fn owed(&self, subscriptions: &[LiveSubscription], routing: Routing<'_>) -> Option<Unsettled> {
        let now = tokio::time::Instant::now();
        let mut expected = vec![0; subscriptions.len()];
        {
            let published = self
                .inner
                .published
                .lock()
                .expect("coordinator published mutex poisoned");
            for publish in published.iter() {
                // A publish is its own broker's: only that broker's subscriptions, and among them
                // only those its routing delivers to, owe it.
                let on: Vec<usize> = (0..subscriptions.len())
                    .filter(|&index| subscriptions[index].scope_id == publish.scope_id)
                    .collect();
                if on.is_empty() {
                    continue;
                }
                let names: Vec<&str> = on
                    .iter()
                    .map(|&index| subscriptions[index].name.as_str())
                    .collect();
                for position in routing(publish.scope_id, publish.message.name(), &names) {
                    if let Some(&index) = on.get(position) {
                        expected[index] += 1;
                    }
                }
            }
        }
        {
            let redeliveries = self
                .inner
                .redeliveries
                .lock()
                .expect("coordinator redeliveries mutex poisoned");
            for (index, subscription) in subscriptions.iter().enumerate() {
                expected[index] += redeliveries
                    .iter()
                    .filter(|due| due.subscription == subscription.id && due.due <= now)
                    .count();
            }
        }
        let records = self
            .inner
            .records
            .lock()
            .expect("coordinator records mutex poisoned");
        let owed = subscriptions
            .iter()
            .zip(expected)
            .find_map(|(subscription, expected)| {
                // Counted by identity, not by name: two subscriptions reporting one name each owe
                // the publish, and one handling it settles nothing for the other.
                let handled = records
                    .iter()
                    .filter(|record| record.subscription == subscription.id)
                    .map(|record| record.deliveries.len())
                    .sum::<usize>();
                (handled < expected).then(|| Unsettled {
                    subscription: subscription.name.clone(),
                    handled,
                    expected,
                })
            });
        drop(records);
        owed
    }

    /// Waits until every subscription in `subscriptions` has handled what it is owed and no
    /// handler is running, or fails once `deadline` has passed.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotSettled`] naming the subscription still owed a delivery, or the
    /// running handler, when the deadline passes first.
    pub(crate) async fn settle_live(
        &self,
        subscriptions: &[LiveSubscription],
        routing: Routing<'_>,
        deadline: Duration,
    ) -> Result<(), TestError> {
        // Why a deadline: a live broker holds the messages in flight, and nothing in this process
        // can read its queues to tell a message still on its way from one that will never arrive.
        let until = tokio::time::Instant::now() + deadline;
        loop {
            // Interest is registered before the state is read, so a record or a settlement landing
            // in between still wakes the wait below.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let owed = self.owed(subscriptions, routing);
            let running = self.inner.in_flight.load(Ordering::SeqCst) > 0;
            if owed.is_none() && !running {
                return Ok(());
            }
            if tokio::time::timeout_at(until, notified).await.is_err() {
                let (subscription, handled, expected) = owed.map_or((None, 0, 0), |owed| {
                    (Some(owed.subscription), owed.handled, owed.expected)
                });
                return Err(TestError::NotSettled {
                    subscription,
                    handled,
                    expected,
                    deadline,
                });
            }
        }
    }

    /// Records one publish made through the `Out` slot named `slot`.
    pub(crate) fn record_slot<Payload: AsRef<[u8]>>(
        &self,
        slot: &'static str,
        msg: &OutgoingMessage<'_, Payload>,
        options: RecordedOptions,
    ) {
        let message = RawMessage::new(msg.name().to_owned(), msg.payload().to_vec())
            .with_headers(msg.headers().clone());
        self.inner
            .slot_records
            .lock()
            .expect("coordinator slot records mutex poisoned")
            .push(SlotRecord {
                slot,
                message,
                options,
            });
    }

    /// Records the per-message options one reply published to `name` carried.
    pub(crate) fn record_reply(&self, name: &str, options: RecordedOptions) {
        self.inner
            .reply_records
            .lock()
            .expect("coordinator reply records mutex poisoned")
            .push((name.to_owned(), options));
    }

    /// The per-message options of every reply published to `name`, in publish order.
    pub(crate) fn reply_published(&self, name: &str) -> Vec<RecordedOptions> {
        self.inner
            .reply_records
            .lock()
            .expect("coordinator reply records mutex poisoned")
            .iter()
            .filter(|(channel, _)| channel == name)
            .map(|(_, options)| options.clone())
            .collect()
    }

    /// Every publish made through the `Out` slot named `slot`, in publish order.
    pub(crate) fn slot_published(&self, slot: &str) -> Vec<SlotRecord> {
        self.inner
            .slot_records
            .lock()
            .expect("coordinator slot records mutex poisoned")
            .iter()
            .filter(|record| record.slot == slot)
            .cloned()
            .collect()
    }

    /// Runs `f` over every record matching `scope_id` and `name`, in delivery order.
    // The guard is held across `f` on purpose: `matching` borrows the records it owns.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn with_records<R>(
        &self,
        scope_id: usize,
        name: &str,
        f: impl FnOnce(&[&Record]) -> R,
    ) -> R {
        let guard = self
            .inner
            .records
            .lock()
            .expect("coordinator records mutex poisoned");
        let matching: Vec<&Record> = guard
            .iter()
            .filter(|r| r.scope_id == scope_id && r.name == name)
            .collect();
        f(&matching)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Subject matching where `*` stands for one dot-separated token.
    fn matches(pattern: &str, destination: &str) -> bool {
        let mut pattern = pattern.split('.');
        let mut subject = destination.split('.');
        loop {
            match (pattern.next(), subject.next()) {
                (None, None) => return true,
                (Some(token), Some(part)) if token == "*" || token == part => {}
                _ => return false,
            }
        }
    }

    /// A broker that delivers to every subscription whose pattern matches, as NATS does.
    fn fan_out(destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        (0..subscriptions.len())
            .filter(|&position| matches(subscriptions[position], destination))
            .collect()
    }

    /// A broker that delivers to the subscription of the destination's own name, and to a
    /// pattern only where there is none.
    fn most_specific(destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let exact: Vec<usize> = (0..subscriptions.len())
            .filter(|&position| subscriptions[position] == destination)
            .collect();
        if exact.is_empty() {
            fan_out(destination, subscriptions)
        } else {
            exact
        }
    }

    fn exact(destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        (0..subscriptions.len())
            .filter(|&position| subscriptions[position] == destination)
            .collect()
    }

    /// One acknowledged delivery through `subscription`.
    fn handled(subscription: &LiveSubscription) -> Record {
        Record {
            scope_id: subscription.scope_id,
            subscription: subscription.id,
            name: subscription.name.clone(),
            deliveries: vec![Delivered {
                raw: Bytes::from_static(b"{}"),
                settle: Some(HandlerResult::Ack),
            }],
            panicked: false,
            decode_failed: false,
        }
    }

    /// East routes with `east`, west with `west`.
    fn brokers_routing(
        east: fn(&str, &[&str]) -> Vec<usize>,
        west: fn(&str, &[&str]) -> Vec<usize>,
    ) -> impl Fn(usize, &str, &[&str]) -> Vec<usize> + Sync {
        move |scope_id, destination, subscriptions| {
            if scope_id == 0 {
                east(destination, subscriptions)
            } else {
                west(destination, subscriptions)
            }
        }
    }

    /// The subscriptions of an app, as `(broker scope, name)` in mount order, each with the
    /// identity its position gives it.
    fn mounted(subscriptions: &[(usize, &str)]) -> Vec<LiveSubscription> {
        subscriptions
            .iter()
            .enumerate()
            .map(|(id, (scope_id, name))| LiveSubscription {
                id,
                scope_id: *scope_id,
                name: (*name).to_owned(),
            })
            .collect()
    }

    /// Two connected brokers, standing for two registrations of one app: what a paired
    /// publisher's origin names.
    struct Brokers {
        east: Box<u8>,
        west: Box<u8>,
    }

    impl Brokers {
        fn new() -> Self {
            Self {
                east: Box::new(0),
                west: Box::new(1),
            }
        }

        /// A coordinator that knows `east` as the registration at 0 and `west` as the one at 1.
        fn coordinator(&self) -> Coordinator {
            let coordinator = Coordinator::new(16);
            coordinator.locate([
                &*self.east as &(dyn Any + Send + Sync),
                &*self.west as &(dyn Any + Send + Sync),
            ]);
            coordinator
        }
    }

    /// Publishes to `name` through a publisher paired against `broker`, from a dispatch of the
    /// registration at `consuming`, and reports the broker took it: the path a reply or an `Out`
    /// slot takes.
    async fn send(coordinator: &Coordinator, consuming: usize, broker: &u8, name: &str) {
        let ((), origin) = paired(broker, async {}).await;
        in_harness_scope(
            Some(HarnessScope::new(coordinator.clone(), consuming, 0)),
            publishing_to(origin, async {
                let msg: OutgoingMessage<'_> = OutgoingMessage::new(name, b"{}".as_slice());
                PipelinePublish::capture(&msg).sent();
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn a_pattern_subscription_is_owed_what_its_pattern_reaches() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(fan_out, exact);
        let subscriptions = mounted(&[(0, "orders.*")]);
        send(&coordinator, 0, &brokers.east, "orders.eu").await;
        send(&coordinator, 0, &brokers.east, "audit").await;

        let owed = coordinator
            .owed(&subscriptions, &routing)
            .expect("the publish to orders.eu is owed to orders.*");
        assert_eq!(
            (owed.subscription.as_str(), owed.handled, owed.expected),
            ("orders.*", 0, 1),
        );

        coordinator.record(handled(&subscriptions[0]));
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
    }

    #[tokio::test]
    async fn a_publish_is_owed_on_the_broker_it_was_paired_against() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(exact, exact);
        let subscriptions = mounted(&[(0, "orders"), (1, "orders")]);
        // A handler on east holding a publisher paired against west publishes to west.
        send(&coordinator, 0, &brokers.west, "orders").await;
        let owed = coordinator
            .owed(&subscriptions, &routing)
            .expect("west's subscription is owed the publish");
        assert_eq!((owed.handled, owed.expected), (0, 1));

        // A delivery on east, the broker that holds the publisher, is not the one it owes.
        coordinator.record(handled(&subscriptions[0]));
        assert!(coordinator.owed(&subscriptions, &routing).is_some());

        coordinator.record(handled(&subscriptions[1]));
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
        assert_eq!(coordinator.published(1, "orders").len(), 1);
        assert!(coordinator.published(0, "orders").is_empty());
    }

    #[tokio::test]
    async fn a_wildcard_and_exact_names_on_two_brokers_are_owed_apart() {
        const EACH: usize = 200;
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(fan_out, exact);
        let subscriptions = mounted(&[(0, "orders.*"), (1, "orders.eu"), (1, "orders.us")]);
        for _ in 0..EACH {
            send(&coordinator, 0, &brokers.west, "orders.eu").await;
            send(&coordinator, 1, &brokers.east, "orders.us").await;
        }
        for _ in 0..EACH - 1 {
            coordinator.record(handled(&subscriptions[1]));
            coordinator.record(handled(&subscriptions[0]));
        }
        coordinator.record(handled(&subscriptions[1]));
        let owed = coordinator
            .owed(&subscriptions, &routing)
            .expect("the last us is still on its way to the wildcard");
        assert_eq!(
            (owed.subscription.as_str(), owed.handled, owed.expected),
            ("orders.*", EACH - 1, EACH),
        );

        coordinator.record(handled(&subscriptions[0]));
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
    }

    #[tokio::test]
    async fn overlapping_subscriptions_owe_what_their_broker_delivers_to_each() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(fan_out, exact);
        let subscriptions = mounted(&[
            (0, "orders.*"),
            (0, "orders.eu"),
            (1, "orders.eu"),
            (1, "orders.us"),
        ]);
        // Both go to east, which delivers eu to both of its subscriptions and us to the wildcard.
        send(&coordinator, 0, &brokers.east, "orders.eu").await;
        send(&coordinator, 0, &brokers.east, "orders.us").await;
        coordinator.record(handled(&subscriptions[0]));
        coordinator.record(handled(&subscriptions[1]));
        let owed = coordinator
            .owed(&subscriptions, &routing)
            .expect("the wildcard is owed both publishes");
        assert_eq!(
            (owed.subscription.as_str(), owed.handled, owed.expected),
            ("orders.*", 1, 2),
        );

        coordinator.record(handled(&subscriptions[0]));
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
    }

    #[tokio::test]
    async fn a_broker_that_picks_one_subscription_owes_only_that_one() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(most_specific, exact);
        let subscriptions = mounted(&[(0, "orders.*"), (0, "orders.eu")]);
        send(&coordinator, 0, &brokers.east, "orders.eu").await;
        send(&coordinator, 0, &brokers.east, "orders.us").await;
        coordinator.record(handled(&subscriptions[1]));
        let owed = coordinator
            .owed(&subscriptions, &routing)
            .expect("us is the wildcard's");
        assert_eq!(
            (owed.subscription.as_str(), owed.handled, owed.expected),
            ("orders.*", 0, 1),
        );

        coordinator.record(handled(&subscriptions[0]));
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
    }

    #[tokio::test]
    async fn two_subscriptions_reporting_one_name_each_owe_the_publish() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(exact, exact);
        // Two subscriptions on one topic, both reporting the topic's name.
        let subscriptions = mounted(&[(0, "orders"), (0, "orders")]);
        send(&coordinator, 0, &brokers.east, "orders").await;
        coordinator.record(handled(&subscriptions[0]));
        let owed = coordinator
            .owed(&subscriptions, &routing)
            .expect("the second subscription has not handled the publish");
        assert_eq!((owed.handled, owed.expected), (0, 1));

        coordinator.record(handled(&subscriptions[1]));
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
    }

    #[tokio::test]
    async fn a_publisher_the_runtime_did_not_pair_is_not_awaited() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(exact, exact);
        let subscriptions = mounted(&[(0, "orders")]);
        in_harness_scope(Some(HarnessScope::new(coordinator.clone(), 0, 0)), async {
            let msg: OutgoingMessage<'_> = OutgoingMessage::new("orders", b"{}".as_slice());
            PipelinePublish::capture(&msg).sent();
        })
        .await;
        assert!(coordinator.owed(&subscriptions, &routing).is_none());
        assert!(coordinator.published(0, "orders").is_empty());
    }

    #[test]
    fn the_debug_form_reports_the_quiescence_counters() {
        let coordinator = Coordinator::new(16);
        let idle = format!("{coordinator:?}");
        assert!(idle.contains("in_flight: 0"), "{idle}");
        assert!(idle.contains("processed: 0"), "{idle}");

        // These two counters are what a hung `drain` is diagnosed from, so Debug must carry them.
        coordinator.enqueued();
        let busy = format!("{coordinator:?}");
        assert!(busy.contains("in_flight: 1"), "{busy}");
    }

    #[tokio::test]
    async fn a_hold_keeps_the_reaction_open_across_a_settle_and_its_copy() {
        let coordinator = Coordinator::new(16);
        // The original delivery is in flight; the retry path takes a hold, then settles it.
        coordinator.enqueued();
        let hold = coordinator.hold();
        coordinator.consumed();

        // The gap before the copy is enqueued: the reaction must not read as settled.
        let mut drive = Box::pin(coordinator.drive());
        assert!(
            futures::poll!(drive.as_mut()).is_pending(),
            "drive returned between the settle and the copy"
        );

        // The copy is counted, the hold released, then the copy settles.
        coordinator.enqueued();
        drop(hold);
        assert!(futures::poll!(drive.as_mut()).is_pending());
        coordinator.consumed();
        drive
            .await
            .expect("the reaction settles once the copy is handled");
    }
}
