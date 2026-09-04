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
}

/// The harness a dispatch task runs under: which coordinator records it, and which broker's
/// registration the delivery belongs to.
#[derive(Clone)]
pub(crate) struct HarnessScope {
    coordinator: Coordinator,
    scope_id: usize,
}

impl HarnessScope {
    pub(crate) fn new(coordinator: Coordinator, scope_id: usize) -> Self {
        Self {
            coordinator,
            scope_id,
        }
    }
}

/// Records a publish made through an `Out` slot against the harness driving the current
/// dispatch task, if any. Called by the slot publisher wrapper on every publish; outside a
/// harness-driven handler (production, or a test without a `TestApp`) it is a no-op.
pub(crate) fn record_slot_publish(slot: &'static str, msg: &OutgoingMessage<'_>) {
    let _ = HARNESS.try_with(|scope| scope.coordinator.record_slot(slot, msg));
}

/// Records one page against the harness driving the current dispatch task, if any: the batch
/// settle path knows the deliveries and their settlements but not which harness (if any) is
/// watching, and it sits behind four call sites whose signatures would otherwise all have to
/// carry one.
pub(crate) fn record_page(name: &str, deliveries: Vec<Delivered>) {
    let _ = HARNESS.try_with(|scope| {
        scope.coordinator.record(Record {
            scope_id: scope.scope_id,
            name: name.to_owned(),
            deliveries,
            panicked: false,
            decode_failed: false,
        });
    });
}

/// One page's record still owed to the harness, held from the moment the settle path captures
/// the page until the record is pushed.
///
/// The settlements a page applies are what release the in-flight count, and the record is
/// written after the last of them, so without this a [`drive`](Coordinator::drive) could return
/// in between and a test would assert against a page that has not been recorded yet.
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

/// One publish made through an `Out` slot: the slot's name and the outgoing message, captured
/// as a [`RawMessage`] (destination, payload, headers).
pub(crate) struct SlotRecord {
    pub(crate) slot: &'static str,
    pub(crate) message: RawMessage,
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

/// One recorded call into a handler: a single delivery, or a whole page.
pub(crate) struct Record {
    /// The broker's registration index in the app, used to scope assertions per broker.
    pub(crate) scope_id: usize,
    /// The subscription (channel) name the message arrived on.
    pub(crate) name: String,
    /// What this call carried: exactly one delivery for a single-message handler, one per
    /// element of the page for a batch handler.
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
    /// A page settling its elements differently has no one outcome; the first element's stands
    /// for the call, which is why the per-element assertions
    /// ([`settled`](super::SubscriberAssertions::settled)) are what a mixed page is read with.
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
}

impl TestHooks {
    /// A hooks slot that is never installed: the production / no-harness path.
    pub(crate) fn detached() -> Self {
        Self {
            coordinator: OnceLock::new(),
        }
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
    timers: Mutex<Vec<Timer>>,
}

/// A scheduled delayed redelivery (`nack_after` / `retry_after`): its deadline and the task that
/// fires it. The harness awaits the due ones when a test advances time.
struct Timer {
    deadline: tokio::time::Instant,
    handle: tokio::task::JoinHandle<()>,
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

    /// Records one publish made through the `Out` slot named `slot`.
    pub(crate) fn record_slot(&self, slot: &'static str, msg: &OutgoingMessage<'_>) {
        let message = RawMessage::new(msg.name().to_owned(), msg.payload().to_vec())
            .with_headers(msg.headers().clone());
        self.inner
            .slot_records
            .lock()
            .expect("coordinator slot records mutex poisoned")
            .push(SlotRecord { slot, message });
    }

    /// Every message published through the `Out` slot named `slot`, in publish order.
    pub(crate) fn slot_published(&self, slot: &str) -> Vec<RawMessage> {
        self.inner
            .slot_records
            .lock()
            .expect("coordinator slot records mutex poisoned")
            .iter()
            .filter(|record| record.slot == slot)
            .map(|record| record.message.clone())
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
}
