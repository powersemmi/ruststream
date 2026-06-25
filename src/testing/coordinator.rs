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

use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::runtime::HandlerResult;

use super::TestError;

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

/// One recorded delivery to a handler.
pub(crate) struct Record {
    /// The broker's registration index in the app, used to scope assertions per broker.
    pub(crate) scope_id: usize,
    /// The subscription (channel) name the message arrived on.
    pub(crate) name: String,
    /// The raw payload bytes the handler received.
    pub(crate) raw: Bytes,
    /// The settlement the dispatcher applied, or `None` when a fail-fast panic left the message
    /// unsettled.
    pub(crate) settle: Option<HandlerResult>,
    /// Whether the handler panicked.
    pub(crate) panicked: bool,
    /// Whether the payload failed to decode before the handler ran.
    pub(crate) decode_failed: bool,
}

impl Record {
    /// Classifies this delivery into a single [`Outcome`]. Panic and decode-failure dominate the
    /// settlement (a fail-fast panic acks nothing; a skip-policy panic still records `Panicked`).
    pub(crate) fn outcome(&self) -> Outcome {
        if self.panicked {
            Outcome::Panicked
        } else if self.decode_failed {
            Outcome::DecodeFailed
        } else {
            match self.settle {
                Some(HandlerResult::Ack) => Outcome::Ack,
                Some(HandlerResult::Nack { requeue: true } | HandlerResult::NackAfter { .. }) => {
                    Outcome::Nack
                }
                Some(HandlerResult::Nack { requeue: false }) | None => Outcome::Drop,
            }
        }
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
    inner: std::sync::Arc<Inner>,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
}

impl Coordinator {
    /// Creates a coordinator that gives up after `max_steps` dispatched deliveries without
    /// reaching quiescence (a guard against perpetual-requeue handlers).
    pub(crate) fn new(max_steps: usize) -> Self {
        Self {
            inner: std::sync::Arc::new(Inner {
                in_flight: AtomicUsize::new(0),
                processed: AtomicUsize::new(0),
                max_steps,
                notify: tokio::sync::Notify::new(),
                records: Mutex::new(Vec::new()),
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
