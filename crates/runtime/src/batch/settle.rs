//! Settling a dispatched batch: the handler's [`BatchResult`] applied to each delivery behind it,
//! and what the harness records of the batch on the way.

use core::mem;
#[cfg(feature = "testing")]
use ruststream_core::__alloc::bytes::Bytes;

use ruststream_core::MaybeSendSync;
use ruststream_core::diag::error;

use super::{BatchResult, UniformFan};
#[cfg(not(ruststream_alloc))]
use super::{Element, Gathers};
use crate::batch::{BatchDrain, BatchVec};
use crate::dispatch::{Delivery, Slot, settle_outcome, settle_reacting};
#[cfg(not(ruststream_alloc))]
use crate::handler::Continuation;
use crate::handler::{HandlerOutcome, HandlerResult};
#[cfg(not(ruststream_alloc))]
use crate::host::KeptCopies;
use crate::host::{HostDetach, PrepareKept, detach_continuation};
#[cfg(not(ruststream_alloc))]
use crate::publish_failure::ErrorHook;
use crate::publish_failure::Reaction;
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
            settle_reacting(
                &mut Slot::new($msg),
                status,
                Some(Reaction::new(error, after)),
                $subscription,
                $delivery,
                $host,
                C::build,
            )
            .await;
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

/// Settles one element of a uniform outcome by the share its fan handed out: an outcome of its
/// own, or the part of the `.error` its mount's fan gathers.
#[cfg(ruststream_alloc)]
macro_rules! settle_slot {
    ($msg:expr, $element:expr, $subscription:expr, $delivery:expr, $host:expr) => {
        settle_element!($msg, $element, $subscription, $delivery, $host)
    };
}

/// See the `alloc` form.
#[cfg(not(ruststream_alloc))]
macro_rules! settle_slot {
    ($msg:expr, $element:expr, $subscription:expr, $delivery:expr, $host:expr) => {
        match $element {
            Element::Whole(outcome) => {
                settle_element!($msg, outcome, $subscription, $delivery, $host)
            }
            Element::Part(status, index) => {
                settle_reacting(
                    &mut Slot::new($msg),
                    status,
                    Some(Reaction::part(index)),
                    $subscription,
                    $delivery,
                    $host,
                    C::build,
                )
                .await;
            }
        }
    };
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
    #[cfg(ruststream_alloc)]
    let mut accepted_results = Settlements::of(result, accepted_len, subscription, false);
    #[cfg(not(ruststream_alloc))]
    let mut accepted_results = Settlements::of(
        result,
        accepted_len,
        subscription,
        host.kept().and_then(KeptCopies::fan).is_some(),
    );
    #[cfg(not(ruststream_alloc))]
    accepted_results.gather(host).await;
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
        #[cfg(ruststream_alloc)]
        let result = accepted_results
            .next()
            .unwrap_or_else(HandlerOutcome::retry);
        #[cfg(not(ruststream_alloc))]
        let result = accepted_results
            .next()
            .unwrap_or_else(|| Element::Whole(HandlerOutcome::retry()));
        #[cfg(feature = "testing")]
        if let Some(log) = log.as_mut() {
            #[cfg(ruststream_alloc)]
            log.settled(result.outcome());
            #[cfg(not(ruststream_alloc))]
            log.settled(result.status());
        }
        settle_slot!(msg, result, subscription, delivery, host);
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
    Each {
        results: BatchDrain<'r, HandlerOutcome>,
        /// The `.error` and `.then` the outcomes share, until the mount's fan takes them.
        #[cfg(not(ruststream_alloc))]
        shared: Option<(ErrorHook, Option<Continuation>)>,
        /// The index of the next element, once the fan holds the shared `.error`.
        #[cfg(not(ruststream_alloc))]
        parts: Option<usize>,
        /// How many elements the fan waits for.
        #[cfg(not(ruststream_alloc))]
        elements: usize,
    },
}

impl<'r> Settlements<'r> {
    // `fanned`: whether the mount gathers a uniform outcome's `.error`, which then leaves the last
    // element for the fan.
    #[cfg_attr(ruststream_alloc, allow(unused_variables))]
    fn of(
        result: &'r mut BatchResult,
        accepted_len: usize,
        subscription: &str,
        fanned: bool,
    ) -> Self {
        match result {
            BatchResult::Uniform(outcome) => Self::Uniform {
                fan: UniformFan::new(mem::replace(outcome, HandlerOutcome::ack()), accepted_len),
                left: accepted_len,
            },
            BatchResult::PerElement(results) => {
                check_outcome_count(results, accepted_len, subscription);
                #[cfg(not(ruststream_alloc))]
                let shared = if fanned {
                    results.last_mut().and_then(HandlerOutcome::take_shared)
                } else {
                    None
                };
                Self::Each {
                    results: results.drain(..),
                    #[cfg(not(ruststream_alloc))]
                    shared,
                    #[cfg(not(ruststream_alloc))]
                    parts: None,
                    #[cfg(not(ruststream_alloc))]
                    elements: accepted_len,
                }
            }
        }
    }

    /// Hands the uniform outcome's `.error` to the mount's fan, when the host has one.
    #[cfg(not(ruststream_alloc))]
    // `Send` exactly where the runner's host is.
    #[allow(clippy::future_not_send)]
    async fn gather<Host: HostDetach>(&mut self, host: &Host) {
        let Some(gatherer) = host.kept().and_then(KeptCopies::fan) else {
            return;
        };
        match self {
            Self::Uniform { fan, left } => fan.gather(host, *left).await,
            Self::Each {
                shared,
                parts,
                elements,
                ..
            } => {
                if let Some((error, after)) = shared.take() {
                    gatherer.begin(error, after, *elements).await;
                    *parts = Some(0);
                }
            }
        }
    }

    #[cfg(ruststream_alloc)]
    fn next(&mut self) -> Option<HandlerOutcome> {
        match self {
            Self::Uniform { fan, left } => {
                *left = left.checked_sub(1)?;
                Some(fan.element(*left == 0))
            }
            Self::Each { results } => results.next(),
        }
    }

    #[cfg(not(ruststream_alloc))]
    fn next(&mut self) -> Option<Element> {
        match self {
            Self::Uniform { fan, left } => {
                *left = left.checked_sub(1)?;
                Some(fan.slot(*left == 0))
            }
            Self::Each { results, parts, .. } => {
                let outcome = results.next()?;
                Some(Element::of(outcome, parts))
            }
        }
    }
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
// One function applies the three shapes of a batch result, and the lint counts the code of the
// build without `alloc` beside the rest; a helper per shape would add a frame to the chain of
// futures, which the recursion limit of an app counts.
#[allow(clippy::cognitive_complexity)]
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
                // Where the mount gathers the outcome, each element carries a part of the
                // `.error`; otherwise the last element carries it.
                #[cfg(not(ruststream_alloc))]
                fan.gather(host, accepted.len()).await;
                let last = accepted.len().saturating_sub(1);
                for (index, msg) in accepted.drain(..).enumerate() {
                    #[cfg(feature = "testing")]
                    batch.settled(status);
                    #[cfg(ruststream_alloc)]
                    let own = fan.element(index == last);
                    #[cfg(not(ruststream_alloc))]
                    let own = fan.slot(index == last);
                    settle_slot!(msg, own, subscription, delivery, host);
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
            // A uniform outcome the macro fanned out leaves its `.error` on the last element
            // for the mount's fan, which then carries it for every element.
            #[cfg(not(ruststream_alloc))]
            let mut parts = share_error(results, accepted.len(), host).await;
            let mut results = results.drain(..);
            for msg in accepted.drain(..) {
                // An unmatched message gets retried: an extra redelivery beats losing it.
                let result = results.next().unwrap_or_else(HandlerOutcome::retry);
                #[cfg(feature = "testing")]
                batch.settled(result.outcome());
                #[cfg(ruststream_alloc)]
                settle_element!(msg, result, subscription, delivery, host);
                #[cfg(not(ruststream_alloc))]
                {
                    let element = Element::of(result, &mut parts);
                    settle_slot!(msg, element, subscription, delivery, host);
                }
            }
        }
    }
    #[cfg(feature = "testing")]
    batch.record(subscription);
}

/// Hands the `.error` a uniform outcome shares between its elements, which the macro left on the
/// last one, to the mount's fan. The index of the first element's part when the fan took it.
#[cfg(not(ruststream_alloc))]
// `Send` exactly where the runner's host is.
#[allow(clippy::future_not_send)]
async fn share_error<Host: HostDetach>(
    results: &mut BatchVec<HandlerOutcome>,
    elements: usize,
    host: &Host,
) -> Option<usize> {
    let gatherer = host.kept().and_then(KeptCopies::fan)?;
    let (error, after) = results.last_mut().and_then(HandlerOutcome::take_shared)?;
    gatherer.begin(error, after, elements).await;
    Some(0)
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
