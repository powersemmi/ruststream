//! Settling a dispatched batch: the handler's [`BatchResult`] applied to each delivery behind it,
//! and what the harness records of the batch on the way.

use core::mem;
#[cfg(feature = "testing")]
use ruststream_core::__alloc::bytes::Bytes;

use ruststream_core::MaybeSendSync;
use ruststream_core::diag::error;

use super::{BatchResult, UniformFan};
use crate::batch::{BatchDrain, BatchVec};
use crate::dispatch::{Delivery, Slot, settle_outcome, settle_reacting};
use crate::handler::{Continuation, HandlerOutcome, HandlerResult};
use crate::host::{HostDetach, PrepareKept, detach_continuation};
use crate::publish_failure::{ErrorHook, Reaction};
use crate::redelivery::RetryCopies;
#[cfg(feature = "testing")]
use crate::testing::coordinator;
#[cfg(feature = "testing")]
use crate::testing::coordinator::Delivered;
#[cfg(feature = "testing")]
use crate::testing::coordinator::PendingRecord;
#[cfg(feature = "testing")]
use crate::testing::coordinator::RecordedValue;
#[cfg(feature = "testing")]
use crate::testing::{Bookkept, harness_section};
use crate::{BuildBatchContext, Settle};

/// Settles one element by its own outcome, as a single delivery is settled: an `.error` registered
/// on the outcome rides the settlement (with the `.and_after` beside it, which then follows the
/// publish the runtime makes), and an outcome without one detaches its continuation once settled.
///
/// The outcome without an `.error` is the common one and settles inline in the caller's loop, as
/// a batch did before the hook existed; only an element that registered one calls out.
macro_rules! settle_element {
    ($msg:expr, $outcome:expr, $subscription:expr, $delivery:expr, $host:expr) => {{
        let (status, after, error) = $outcome.into_parts();
        if let Some(error) = error {
            settle_erroring($msg, status, after, error, $subscription, $delivery, $host).await;
        } else {
            settle_outcome(
                &mut Slot::new($msg),
                status,
                $subscription,
                $delivery,
                $host,
                C::build,
            )
            .await;
            // The continuation runs after this element is settled, on the tracked set so a
            // graceful shutdown drains it. At-most-once: a lost or panicking continuation never
            // redelivers the already-settled message.
            if let Some(after) = after {
                detach_continuation!($host, after);
            }
        }
    }};
}

/// Settles a batch whose rejected elements were deferred past the handler call: each rejected
/// index settles by its recorded outcome, and the accepted remainder settles by the handler's
/// [`BatchResult`] in order, exactly as [`settle_batch`] applies it.
// `Send` exactly where the runner's host is: a multi-thread runner's `Detach` is `Sync`.
#[allow(clippy::future_not_send)]
pub(super) async fn settle_split_batch<M, C, Host>(
    batch: &mut BatchVec<M>,
    rejected: BatchVec<(usize, HandlerResult)>,
    result: &mut BatchResult,
    subscription: &str,
    delivery: &Delivery<C>,
    host: &Host,
) where
    M: Settle,
    C: BuildBatchContext<M> + MaybeSendSync + 'static,
    Host: HostDetach<Kept: PrepareKept<C>, Retry: RetryCopies<C>>,
{
    let accepted_len = batch.len() - rejected.len();
    // Fan the uniform outcome out over the accepted elements only; the rejects keep their own
    // settlements.
    let mut accepted_results = Settlements::of(result, accepted_len, subscription);
    // The body saw the accepted elements, so they are the call the harness records. A batch that
    // refused every element never reached the body, and records nothing.
    #[cfg(feature = "testing")]
    let mut log = (accepted_len > 0).then(|| BatchLog::of_accepted(batch, &rejected));
    let mut rejected = rejected.into_iter().peekable();
    for (index, msg) in batch.drain(..).enumerate() {
        if rejected.peek().is_some_and(|(at, _)| *at == index) {
            let (_, outcome) = rejected.next().expect("peeked");
            settle_outcome(
                &mut Slot::new(msg),
                outcome,
                subscription,
                delivery,
                host,
                C::build,
            )
            .await;
            continue;
        }
        let result = accepted_results
            .next()
            .unwrap_or_else(HandlerOutcome::retry);
        #[cfg(feature = "testing")]
        if let Some(log) = log.as_mut() {
            log.settled(result.outcome());
        }
        settle_element!(msg, result, subscription, delivery, host);
    }
    #[cfg(feature = "testing")]
    if let Some(log) = log {
        log.record(subscription);
    }
}

/// The outcomes of a split batch's accepted elements, in order. The handler's result stays where
/// the caller holds it: a batch's outcomes are the largest thing a settling carries, and every
/// frame of the settling that owned a copy would keep a full-size one for the whole batch.
enum Settlements<'r> {
    /// One outcome handed out element by element.
    Uniform { fan: UniformFan, left: usize },
    /// The handler's own outcomes, one per element.
    Each(BatchDrain<'r, HandlerOutcome>),
}

impl<'r> Settlements<'r> {
    fn of(result: &'r mut BatchResult, accepted_len: usize, subscription: &str) -> Self {
        match result {
            BatchResult::Uniform(outcome) => Self::Uniform {
                fan: UniformFan::new(mem::replace(outcome, HandlerOutcome::ack()), accepted_len),
                left: accepted_len,
            },
            BatchResult::PerElement(results) => {
                check_outcome_count(results, accepted_len, subscription);
                Self::Each(results.drain(..))
            }
        }
    }

    fn next(&mut self) -> Option<HandlerOutcome> {
        match self {
            Self::Uniform { fan, left } => {
                *left = left.checked_sub(1)?;
                Some(fan.element(*left == 0))
            }
            Self::Each(results) => results.next(),
        }
    }
}

/// Settles one element whose outcome registered an `.error`: the hook rides the settlement, and
/// the continuation beside it follows the publish the runtime makes.
// `Send` exactly where the runner's host is: a multi-thread runner's `Detach` is `Sync`.
#[allow(clippy::future_not_send, clippy::too_many_arguments)]
async fn settle_erroring<M, C, Host>(
    msg: M,
    status: HandlerResult,
    after: Option<Continuation>,
    error: ErrorHook,
    subscription: &str,
    delivery: &Delivery<C>,
    host: &Host,
) where
    M: Settle,
    C: BuildBatchContext<M> + MaybeSendSync + 'static,
    Host: HostDetach<Kept: PrepareKept<C>, Retry: RetryCopies<C>>,
{
    let reaction = Some(Reaction::new(error, after));
    settle_reacting(
        &mut Slot::new(msg),
        status,
        reaction,
        subscription,
        delivery,
        host,
        C::build,
    )
    .await;
}

/// Applies one [`BatchResult`] to the accepted deliveries behind it - uniformly, or element by
/// element, spawning each per-element continuation on the tracked set after its settle.
// `Send` exactly where the runner's host is: a multi-thread runner's `Detach` is `Sync`.
#[allow(clippy::future_not_send)]
pub(crate) fn settle_batch<'a, M, C, Host>(
    accepted: &'a mut BatchVec<M>,
    result: &'a mut BatchResult,
    subscription: &'a str,
    delivery: &'a Delivery<C>,
    host: &'a Host,
) -> impl Future<Output = ()>
where
    M: Settle,
    C: BuildBatchContext<M> + MaybeSendSync + 'static,
    Host: HostDetach<Kept: PrepareKept<C>, Retry: RetryCopies<C>>,
{
    // Not an `async fn`: a frame of its own would sit between the caller and the settling for
    // nothing.
    settle_lent_batch(
        accepted,
        result,
        subscription,
        delivery,
        host,
        lent_nothing(),
    )
}

/// Warns where the handler returned a different number of outcomes than the batch has elements:
/// an unmatched delivery is retried, and an outcome past the last delivery is dropped.
#[inline]
fn check_outcome_count(results: &BatchVec<HandlerOutcome>, expected: usize, subscription: &str) {
    if results.len() != expected {
        error!(
            target: "ruststream::dispatch",
            subscription = %subscription,
            expected,
            returned = results.len(),
            "per-element outcome count does not match the batch; \
             retrying the unmatched remainder",
        );
    }
}

/// What the harness records of the values a batch lent its handler, one per accepted delivery
/// in order: empty where the batch lent none, and nothing at all outside the `testing` feature.
#[cfg(feature = "testing")]
pub(crate) type LentValues = BatchVec<RecordedValue>;

/// See the `testing` counterpart: without the harness there is nothing to record. A unit struct
/// rather than `()`, so a form that lends nothing hands it on as a value like any other.
#[cfg(not(feature = "testing"))]
#[derive(Debug)]
pub(crate) struct LentValues;

/// The record of a batch that lent its handler no values.
#[cfg(feature = "testing")]
pub(crate) const fn lent_nothing() -> LentValues {
    BatchVec::new()
}

/// See the `testing` counterpart.
#[cfg(not(feature = "testing"))]
pub(crate) const fn lent_nothing() -> LentValues {
    LentValues
}

/// [`settle_batch`] for a batch that lent its handler the values its deliveries carry: the
/// harness records each value beside the delivery it belongs to.
// `Send` exactly where the runner's host is: a multi-thread runner's `Detach` is `Sync`.
#[allow(clippy::future_not_send)]
pub(crate) async fn settle_lent_batch<M, C, Host>(
    accepted: &mut BatchVec<M>,
    result: &mut BatchResult,
    subscription: &str,
    delivery: &Delivery<C>,
    host: &Host,
    lent: LentValues,
) where
    M: Settle,
    C: BuildBatchContext<M> + MaybeSendSync + 'static,
    Host: HostDetach<Kept: PrepareKept<C>, Retry: RetryCopies<C>>,
{
    // Every batch form funnels its batch through here, which is the one place that knows both
    // the deliveries and the settlements they got; the harness reads the batch off it.
    #[cfg(feature = "testing")]
    let mut batch = BatchLog::of(accepted.iter(), lent);
    #[cfg(not(feature = "testing"))]
    let LentValues = lent;
    match result {
        BatchResult::Uniform(outcome) => {
            let (status, after, error) = mem::replace(outcome, HandlerOutcome::ack()).into_parts();
            if let Some(error) = error {
                // The one uniform outcome fans out: each element settles by the status and owns
                // a share of the `.error`, which runs for a copy the runtime fails to publish
                // for it; the `.error` runs once with every failure, and the continuation once
                // when at least one publish succeeded, both after the last element's publish is
                // reported (at-most-once, like the per-element ones).
                let mut own = HandlerOutcome::from(status).error_hook(error);
                if let Some(after) = after {
                    own = own.and_after_boxed(after);
                }
                let mut fan = UniformFan::new(own, accepted.len());
                let last = accepted.len().saturating_sub(1);
                for (index, msg) in accepted.drain(..).enumerate() {
                    #[cfg(feature = "testing")]
                    batch.settled(status);
                    let own = fan.element(index == last);
                    settle_element!(msg, own, subscription, delivery, host);
                }
            } else {
                for msg in accepted.drain(..) {
                    #[cfg(feature = "testing")]
                    batch.settled(status);
                    settle_outcome(
                        &mut Slot::new(msg),
                        status,
                        subscription,
                        delivery,
                        host,
                        C::build,
                    )
                    .await;
                }
                // The one uniform continuation runs after the whole batch is settled, on the
                // tracked set so a graceful shutdown drains it (at-most-once, like the
                // per-element ones below).
                if let Some(after) = after {
                    detach_continuation!(host, after);
                }
            }
        }
        BatchResult::PerElement(results) => {
            check_outcome_count(results, accepted.len(), subscription);
            let mut results = results.drain(..);
            for msg in accepted.drain(..) {
                // An unmatched message gets retried: an extra redelivery beats losing it.
                let result = results.next().unwrap_or_else(HandlerOutcome::retry);
                #[cfg(feature = "testing")]
                batch.settled(result.outcome());
                settle_element!(msg, result, subscription, delivery, host);
            }
        }
    }
    #[cfg(feature = "testing")]
    batch.record(subscription);
}

/// The batch a settle is applying, captured for the harness: the payloads before the
/// deliveries are consumed, then one settlement per element as it is applied.
///
/// The settlements are what release the harness's in-flight count, and the record is only
/// complete once the last of them is applied - so the log holds a count of its own for as long
/// as it lives, and a quiescence wait cannot return between the final settle and the record.
#[cfg(feature = "testing")]
struct BatchLog {
    deliveries: Bookkept<BatchVec<Delivered>>,
    settled: usize,
    _pending: PendingRecord,
}

#[cfg(feature = "testing")]
impl BatchLog {
    fn of<'m, M: Settle + 'm>(accepted: impl IntoIterator<Item = &'m M>, lent: LentValues) -> Self {
        let mut lent = lent.into_iter();
        Self {
            deliveries: harness_section(|| {
                Bookkept::new(
                    accepted
                        .into_iter()
                        .map(|msg| Delivered {
                            raw: Bytes::copy_from_slice(msg.payload()),
                            value: lent.next(),
                            settle: None,
                        })
                        .collect(),
                )
            }),
            settled: 0,
            _pending: PendingRecord::new(),
        }
    }

    /// The log of a split batch: the deliveries the body saw, which are the ones not `rejected`.
    fn of_accepted<M: Settle>(batch: &[M], rejected: &[(usize, HandlerResult)]) -> Self {
        Self::of(
            batch
                .iter()
                .enumerate()
                .filter(|(index, _)| rejected.binary_search_by_key(index, |&(at, _)| at).is_err())
                .map(|(_, msg)| msg),
            lent_nothing(),
        )
    }

    fn settled(&mut self, status: HandlerResult) {
        if let Some(one) = self.deliveries.get_mut(self.settled) {
            one.settle = Some(status);
        }
        self.settled += 1;
    }

    fn record(self, subscription: &str) {
        coordinator::record_batch(subscription, self.deliveries.into_inner());
    }
}
