//! The recording-and-quiescence [`Coordinator`] shared by the harness, the dispatch path, and a
//! broker's in-process bus.
//!
//! One `Coordinator` is created per [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) run and installed into both the
//! dispatch `Delivery` context (via [`TestHooks`]) and each
//! broker's bus. It does two jobs:
//!
//! - Records every delivery a handler saw: the raw payload, headers, and the final outcome
//!   ([`Outcome`]), keyed by the broker's scope id and the subscription name.
//! - Tracks in-flight work so [`TestApp::publish`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) can drive the system to a
//!   standstill before returning: every enqueue into a subscriber increments the counter, every
//!   completed dispatch decrements it.

use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::Mutex;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Instant;

pub(crate) use crate::pairing::{Origin, paired};
use crate::{OutgoingMessage, RawMessage};

use super::TestError;
use super::attribution::{PublishedEntry, RecordedOptions, RuntimePublish, Span};
use super::hooks::{DueCall, DuePublish, HookRunner, Hooks, PublishTarget, Within};

mod scope;

pub(crate) use scope::{
    Delivered, HarnessScope, PendingRecord, PipelinePublish, Record, RecordedValue, SlotRecord,
    calls_in_harness_scope, in_harness_scope, publishing_to, record_batch, record_slot_publish,
};
pub use scope::{Outcome, TestHooks};

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

/// How many publishes the record holds before it first grows: a test that counts what its own
/// publishes allocate (each records one entry) measures the publishes, not the record's growth.
const PUBLISHED_UP_FRONT: usize = 1024;

struct Inner {
    in_flight: AtomicUsize,
    processed: AtomicUsize,
    max_steps: usize,
    notify: Notify,
    records: Mutex<Vec<Record>>,
    slot_records: Mutex<Vec<SlotRecord>>,
    /// The harness's clock: ticks once when a runtime publish is captured and once when the
    /// broker took it, so the records tell which publishes were in flight at the same time.
    clock: AtomicU64,
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
    hooks: Hooks,
}

/// One publish the harness saw, and the broker it went to: the one the test named, or the one
/// the publisher was paired against.
struct Published {
    scope_id: usize,
    entry: PublishedEntry,
}

/// A subscription a live settle waits on: its identity, the broker it belongs to, and its name.
#[derive(Clone, Debug)]
pub struct LiveSubscription {
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
    due: Instant,
}

/// A scheduled delayed redelivery (`nack_after` / `retry_after`): its deadline and the task that
/// fires it. The harness awaits the due ones when a test advances time.
struct Timer {
    deadline: Instant,
    handle: JoinHandle<()>,
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
    #[doc(hidden)]
    #[must_use]
    pub fn new(max_steps: usize) -> Self {
        Self::with_hooks(max_steps, HookRunner::Thread)
    }

    /// Creates a coordinator for an app that runs as one future inside the test's own: as
    /// [`new`](Self::new), and a hook runs inside the future that recorded its event, so the
    /// test's step that polls the app runs it.
    #[doc(hidden)]
    #[must_use]
    pub fn in_place(max_steps: usize) -> Self {
        Self::with_hooks(max_steps, HookRunner::InPlace)
    }

    fn with_hooks(max_steps: usize, runner: HookRunner) -> Self {
        Self {
            inner: Arc::new(Inner {
                in_flight: AtomicUsize::new(0),
                processed: AtomicUsize::new(0),
                max_steps,
                notify: Notify::new(),
                records: Mutex::new(Vec::new()),
                slot_records: Mutex::new(Vec::new()),
                clock: AtomicU64::new(0),
                published: Mutex::new(Vec::with_capacity(PUBLISHED_UP_FRONT)),
                brokers: Mutex::new(Vec::new()),
                redeliveries: Mutex::new(Vec::new()),
                timers: Mutex::new(Vec::new()),
                hooks: Hooks::new(runner),
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
    ///
    /// The hooks registered on the subscription's handle are noted with it, to run once the
    /// dispatch settled the call ([`run_due_calls`](Self::run_due_calls)).
    ///
    /// Returns the call's hooks, read up to this call, for the dispatch to run once it settled it.
    pub(crate) fn record(&self, record: Record) -> Option<DueCall> {
        let hooks = self.inner.hooks.on_calls_of(record.scope_id, &record.name);
        let call = hooks.map(|hooks| (record.scope_id, record.name.clone(), hooks));
        let mut records = self
            .inner
            .records
            .lock()
            .expect("coordinator records mutex poisoned");
        records.push(record);
        let within = Within::first(records.len());
        drop(records);
        call.map(|(scope_id, name, hooks)| DueCall::new(scope_id, name, within, hooks))
    }

    /// The hooks a test registered on this run's handles.
    #[doc(hidden)]
    #[must_use]
    pub fn hooks(&self) -> &Hooks {
        &self.inner.hooks
    }

    /// Schedules a delayed redelivery (`nack_after` / `retry_after`): after `delay`, `redeliver`
    /// runs (it must re-enqueue the message and call [`enqueued`](Self::enqueued)). The redelivery is
    /// off the synchronous reaction the harness drives, so a publish returns once the immediate
    /// settlement is recorded; a test advances time with [`TestApp::advance`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) to
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
        let deadline = Instant::now() + delay;
        let handle = tokio::spawn(async move {
            time::sleep(delay).await;
            redeliver();
        });
        self.push_timer(deadline, handle);
    }

    /// The awaitable form of [`schedule_redelivery`](Self::schedule_redelivery), for a redelivery
    /// that has to await something itself: the runtime's deferred `retry_after` fallback
    /// re-publishes the message, and the publish is what re-enqueues it. Awaiting the whole
    /// future inside the timer is what makes the copy counted by the time
    /// [`TestApp::advance`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) drives the reaction.
    ///
    /// # Panics
    ///
    /// Panics if the internal timers mutex was poisoned by an earlier panic while it was held.
    pub(crate) fn schedule_redelivery_future<Fut>(&self, delay: Duration, redeliver: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let deadline = Instant::now() + delay;
        let handle = tokio::spawn(async move {
            time::sleep(delay).await;
            redeliver.await;
        });
        self.push_timer(deadline, handle);
    }

    /// Records one scheduled redelivery so [`fire_due_timers`](Self::fire_due_timers) can await it
    /// once its deadline passes.
    fn push_timer(&self, deadline: Instant, handle: JoinHandle<()>) {
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
    #[doc(hidden)]
    pub async fn fire_due_timers(&self) {
        let now = Instant::now();
        let due: Vec<JoinHandle<()>> = {
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

    /// Whether `max_steps` deliveries were dispatched and one is still in flight: the reaction
    /// does not converge. A broker whose app runs inside the test's own future stops handing out
    /// deliveries once this holds, so the step that polls the app returns and reports it.
    #[doc(hidden)]
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.inner.processed.load(Ordering::SeqCst) >= self.inner.max_steps
            && self.inner.in_flight.load(Ordering::SeqCst) > 0
    }

    /// Waits until no message is in flight, or fails once `max_steps` deliveries have been
    /// dispatched without settling (a non-converging reaction).
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] when the step budget is exhausted before the reaction
    /// settles.
    #[doc(hidden)]
    pub async fn drive(&self) -> Result<(), TestError> {
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
    #[doc(hidden)]
    pub fn locate<'a>(&self, connected: impl IntoIterator<Item = &'a (dyn Any + Send + Sync)>) {
        *self
            .inner
            .brokers
            .lock()
            .expect("coordinator brokers mutex poisoned") = connected
            .into_iter()
            .enumerate()
            .filter_map(|(scope_id, connected)| {
                Origin::of::<dyn Any + Send + Sync>(connected)
                    .address()
                    .map(|address| (address, scope_id))
            })
            .collect();
    }

    /// Notes the connected broker at `origin` as the one registered at `scope_id`, for a run whose
    /// brokers connect one by one and are not `Send`.
    pub(crate) fn locate_local(&self, origin: Origin, scope_id: usize) {
        if let Some(address) = origin.address() {
            self.inner
                .brokers
                .lock()
                .expect("coordinator brokers mutex poisoned")
                .push((address, scope_id));
        }
    }

    /// The clock as the test starts a publish of its own, for
    /// [`record_published`](Self::record_published) once the broker took it.
    #[doc(hidden)]
    #[must_use]
    pub fn test_publish_started(&self) -> u64 {
        self.tick()
    }

    /// Records one publish the test made onto the broker registered at `scope_id`, started at
    /// `started` on the harness's clock: the broker took it. Returns the hooks registered on its
    /// destination.
    #[doc(hidden)]
    pub fn record_published(
        &self,
        scope_id: usize,
        message: RawMessage,
        started: u64,
    ) -> DuePublish {
        let span = Span {
            started,
            finished: self.tick(),
        };
        self.push_published(Published {
            scope_id,
            entry: PublishedEntry::Test { message, span },
        })
    }

    fn push_published(&self, published: Published) -> DuePublish {
        let scope_id = published.scope_id;
        let mut record = self
            .inner
            .published
            .lock()
            .expect("coordinator published mutex poisoned");
        record.push(published);
        let within = Within::first(record.len());
        let entry = &record.last().expect("just recorded").entry;
        let hooks = self.inner.hooks.published(
            self,
            || PublishTarget::Channel {
                scope_id,
                name: entry.message().name().to_owned(),
            },
            within,
        );
        drop(record);
        self.inner.notify.notify_waiters();
        hooks
    }

    /// The next reading of the harness's clock.
    fn tick(&self) -> u64 {
        self.inner.clock.fetch_add(1, Ordering::SeqCst)
    }

    /// Records one publish the app handed a broker, on the broker its publisher was paired
    /// against. A publisher the runtime did not pair names no broker, and its publish is left out:
    /// the harness neither waits for it nor lists it, and no hook runs for it.
    fn record_sent(&self, origin: Origin, publish: RuntimePublish) -> DuePublish {
        let scope_id = origin.address().and_then(|address| {
            self.inner
                .brokers
                .lock()
                .expect("coordinator brokers mutex poisoned")
                .iter()
                .find(|(known, _)| *known == address)
                .map(|(_, scope_id)| *scope_id)
        });
        scope_id.map_or(DuePublish::NONE, |scope_id| {
            self.push_published(Published {
                scope_id,
                entry: PublishedEntry::Runtime(publish),
            })
        })
    }

    /// Every publish to `name` the harness saw go to the broker registered at `scope_id`, in
    /// publish order.
    #[doc(hidden)]
    #[must_use]
    pub fn published(&self, scope_id: usize, name: &str) -> Vec<PublishedEntry> {
        self.published_within(scope_id, name, Within::ALL)
    }

    /// Like [`published`](Self::published), over the publishes recorded up to `within`.
    #[doc(hidden)]
    #[must_use]
    pub fn published_within(
        &self,
        scope_id: usize,
        name: &str,
        within: Within,
    ) -> Vec<PublishedEntry> {
        self.inner
            .published
            .lock()
            .expect("coordinator published mutex poisoned")
            .iter()
            .take(within.len())
            .filter(|published| {
                published.scope_id == scope_id && published.entry.message().name() == name
            })
            .map(|published| published.entry.clone())
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
                due: Instant::now() + delay,
            });
        self.inner.notify.notify_waiters();
    }

    /// The first subscription still owed a delivery, with what it has handled and what it is owed:
    /// every publish its broker delivers to it, as that broker's routing says, and every
    /// redelivery due to it by now.
    fn owed(&self, subscriptions: &[LiveSubscription], routing: Routing<'_>) -> Option<Unsettled> {
        let now = Instant::now();
        let mut expected = self.routed_publishes(subscriptions, routing);
        self.add_due_redeliveries(subscriptions, now, &mut expected);
        self.first_unsettled(subscriptions, expected)
    }

    /// How many recorded publishes each subscription is owed, in `subscriptions` order: a publish
    /// counts for the subscriptions of its own broker that the broker's routing delivers it to.
    fn routed_publishes(
        &self,
        subscriptions: &[LiveSubscription],
        routing: Routing<'_>,
    ) -> Vec<usize> {
        let mut expected = vec![0; subscriptions.len()];
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
            for position in routing(publish.scope_id, publish.entry.message().name(), &names) {
                if let Some(&index) = on.get(position) {
                    expected[index] += 1;
                }
            }
        }
        drop(published);
        expected
    }

    /// Adds to `expected` every redelivery due to each subscription by `now`.
    fn add_due_redeliveries(
        &self,
        subscriptions: &[LiveSubscription],
        now: Instant,
        expected: &mut [usize],
    ) {
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

    /// The first subscription that has handled fewer deliveries than `expected` says it is owed.
    fn first_unsettled(
        &self,
        subscriptions: &[LiveSubscription],
        expected: Vec<usize>,
    ) -> Option<Unsettled> {
        let records = self
            .inner
            .records
            .lock()
            .expect("coordinator records mutex poisoned");
        subscriptions
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
            })
    }

    /// Waits until every subscription in `subscriptions` has handled what it is owed and no
    /// handler is running, or fails once `deadline` has passed.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotSettled`] naming the subscription still owed a delivery, or the
    /// running handler, when the deadline passes first.
    #[doc(hidden)]
    pub async fn settle_live(
        &self,
        subscriptions: &[LiveSubscription],
        routing: Routing<'_>,
        deadline: Duration,
    ) -> Result<(), TestError> {
        // Why a deadline: a live broker holds the messages in flight, and nothing in this process
        // can read its queues to tell a message still on its way from one that will never arrive.
        let until = Instant::now() + deadline;
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
            if time::timeout_at(until, notified).await.is_err() {
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

    /// Records one publish made through the `Out` slot named `slot`, and returns the hooks
    /// registered on the slot's view.
    pub(crate) fn record_slot<Payload: AsRef<[u8]>>(
        &self,
        slot: &'static str,
        msg: &OutgoingMessage<'_, Payload>,
        options: RecordedOptions,
    ) -> DuePublish {
        let message = RawMessage::new(msg.name().to_owned(), msg.payload().to_vec())
            .with_headers(msg.headers().clone());
        let within = {
            let mut records = self
                .inner
                .slot_records
                .lock()
                .expect("coordinator slot records mutex poisoned");
            records.push(SlotRecord {
                slot,
                message,
                options,
            });
            Within::first(records.len())
        };
        self.inner
            .hooks
            .published(self, || PublishTarget::Slot(slot), within)
    }

    /// Every publish made through the `Out` slot named `slot` up to `within`, in publish order.
    pub(crate) fn slot_published(&self, slot: &str, within: Within) -> Vec<SlotRecord> {
        self.inner
            .slot_records
            .lock()
            .expect("coordinator slot records mutex poisoned")
            .iter()
            .take(within.len())
            .filter(|record| record.slot == slot)
            .cloned()
            .collect()
    }

    /// Runs `f` over every record matching `scope_id` and `name` up to `within`, in delivery order.
    // The guard is held across `f` on purpose: `matching` borrows the records it owns.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn with_records<R>(
        &self,
        scope_id: usize,
        name: &str,
        within: Within,
        f: impl FnOnce(&[&Record]) -> R,
    ) -> R {
        let guard = self
            .inner
            .records
            .lock()
            .expect("coordinator records mutex poisoned");
        let matching: Vec<&Record> = guard
            .iter()
            .take(within.len())
            .filter(|r| r.scope_id == scope_id && r.name == name)
            .collect();
        f(&matching)
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use alloc::format;
    use bytes::Bytes;

    use super::*;
    use crate::HandlerResult;

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
                value: None,
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
            let registrations: [&(dyn Any + Send + Sync); 2] = [&*self.east, &*self.west];
            coordinator.locate(registrations);
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
                PipelinePublish::capture::<_, ()>(&msg, None).sent().await;
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

    /// A publish is owed on the broker its publisher was paired against, not on the broker that
    /// holds the handler which published it.
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
    async fn a_publisher_the_runtime_did_not_pair_is_not_awaited() {
        let brokers = Brokers::new();
        let coordinator = brokers.coordinator();
        let routing = brokers_routing(exact, exact);
        let subscriptions = mounted(&[(0, "orders")]);
        in_harness_scope(Some(HarnessScope::new(coordinator.clone(), 0, 0)), async {
            let msg: OutgoingMessage<'_> = OutgoingMessage::new("orders", b"{}".as_slice());
            PipelinePublish::capture::<_, ()>(&msg, None).sent().await;
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
