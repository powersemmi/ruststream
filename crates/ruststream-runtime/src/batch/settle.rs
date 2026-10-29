//! Settling a dispatched batch: the handler's [`BatchResult`] applied to each delivery behind it,
//! and what the harness records of the batch on the way.

use tracing::error;

use super::{BatchResult, uniform_batch};
use crate::dispatch::{Delivery, Slot, settle_outcome};
use crate::handler::{HandlerOutcome, HandlerResult};
#[cfg(feature = "testing")]
use crate::testing::coordinator;
#[cfg(feature = "testing")]
use crate::testing::coordinator::Delivered;
#[cfg(feature = "testing")]
use crate::testing::coordinator::PendingRecord;
#[cfg(feature = "testing")]
use crate::testing::coordinator::RecordedValue;
use crate::{BuildBatchContext, IncomingMessage};

/// Settles a batch whose rejected elements were deferred past the handler call: each rejected
/// index settles by its recorded outcome, and the accepted remainder settles by the handler's
/// [`BatchResult`] in order, exactly as [`settle_batch`] applies it.
pub(super) async fn settle_split_batch<M, C>(
    batch: Vec<M>,
    rejected: Vec<(usize, HandlerResult)>,
    result: BatchResult,
    subscription: &str,
    delivery: &Delivery<C>,
) where
    M: IncomingMessage,
    C: BuildBatchContext<M> + Send + Sync + 'static,
{
    if rejected.is_empty() {
        return settle_batch(batch, result, subscription, delivery).await;
    }
    let accepted_len = batch.len() - rejected.len();
    let per_element = match result {
        // Fan the uniform outcome out over the accepted elements only; the rejects keep their
        // own settlements.
        BatchResult::Uniform(outcome) => uniform_batch(outcome, accepted_len),
        BatchResult::PerElement(results) => {
            if results.len() != accepted_len {
                error!(
                    target: "ruststream::dispatch",
                    subscription = %subscription,
                    expected = accepted_len,
                    returned = results.len(),
                    "per-element outcome count does not match the batch; \
                     retrying the unmatched remainder",
                );
            }
            results
        }
    };
    // The body saw the accepted elements, so they are the call the harness records. A batch that
    // refused every element never reached the body, and records nothing.
    #[cfg(feature = "testing")]
    let mut log = (accepted_len > 0).then(|| {
        BatchLog::of(
            batch
                .iter()
                .enumerate()
                .filter(|(index, _)| rejected.binary_search_by_key(index, |&(at, _)| at).is_err())
                .map(|(_, msg)| msg),
            LentValues::default(),
        )
    });
    let mut accepted_results = per_element.into_iter();
    let mut rejected = rejected.into_iter().peekable();
    for (index, msg) in batch.into_iter().enumerate() {
        if rejected.peek().is_some_and(|(at, _)| *at == index) {
            let (_, outcome) = rejected.next().expect("peeked");
            settle_outcome(
                &mut Slot::new(msg),
                outcome,
                subscription,
                delivery,
                C::build,
            )
            .await;
            continue;
        }
        let mut result = accepted_results
            .next()
            .unwrap_or_else(HandlerOutcome::retry);
        let after = result.take_after();
        #[cfg(feature = "testing")]
        if let Some(log) = log.as_mut() {
            log.settled(result.outcome());
        }
        settle_outcome(
            &mut Slot::new(msg),
            result.outcome(),
            subscription,
            delivery,
            C::build,
        )
        .await;
        if let Some(after) = after {
            delivery.spawn_after(after);
        }
    }
    #[cfg(feature = "testing")]
    if let Some(log) = log {
        log.record(subscription);
    }
}

/// Applies one [`BatchResult`] to the accepted deliveries behind it - uniformly, or element by
/// element, spawning each per-element continuation on the tracked set after its settle.
pub(crate) async fn settle_batch<M, C>(
    accepted: Vec<M>,
    result: BatchResult,
    subscription: &str,
    delivery: &Delivery<C>,
) where
    M: IncomingMessage,
    C: BuildBatchContext<M> + Send + Sync + 'static,
{
    settle_lent_batch(
        accepted,
        result,
        subscription,
        delivery,
        LentValues::default(),
    )
    .await;
}

/// What the harness records of the values a batch lent its handler, one per accepted delivery
/// in order: empty where the batch lent none, and nothing at all outside the `testing` feature.
#[cfg(feature = "testing")]
pub(crate) type LentValues = Vec<RecordedValue>;

/// See the `testing` counterpart: without the harness there is nothing to record.
#[cfg(not(feature = "testing"))]
pub(crate) type LentValues = ();

/// [`settle_batch`] for a batch that lent its handler the values its deliveries carry: the
/// harness records each value beside the delivery it belongs to.
pub(crate) async fn settle_lent_batch<M, C>(
    accepted: Vec<M>,
    result: BatchResult,
    subscription: &str,
    delivery: &Delivery<C>,
    lent: LentValues,
) where
    M: IncomingMessage,
    C: BuildBatchContext<M> + Send + Sync + 'static,
{
    // Every batch form funnels its batch through here, which is the one place that knows both
    // the deliveries and the settlements they got; the harness reads the batch off it.
    #[cfg(feature = "testing")]
    let mut batch = BatchLog::of(&accepted, lent);
    #[cfg(not(feature = "testing"))]
    let () = lent;
    match result {
        BatchResult::Uniform(mut outcome) => {
            let after = outcome.take_after();
            let status = outcome.outcome();
            for msg in accepted {
                #[cfg(feature = "testing")]
                batch.settled(status);
                settle_outcome(
                    &mut Slot::new(msg),
                    status,
                    subscription,
                    delivery,
                    C::build,
                )
                .await;
            }
            // The one uniform continuation runs after the whole batch is settled, on the
            // tracked set so a graceful shutdown drains it (at-most-once, like the
            // per-element ones below).
            if let Some(after) = after {
                delivery.spawn_after(after);
            }
        }
        BatchResult::PerElement(results) => {
            if results.len() != accepted.len() {
                error!(
                    target: "ruststream::dispatch",
                    subscription = %subscription,
                    expected = accepted.len(),
                    returned = results.len(),
                    "per-element outcome count does not match the batch; \
                     retrying the unmatched remainder",
                );
            }
            let mut results = results.into_iter();
            for msg in accepted {
                // An unmatched message gets retried: an extra redelivery beats losing it.
                let mut result = results.next().unwrap_or_else(HandlerOutcome::retry);
                let after = result.take_after();
                #[cfg(feature = "testing")]
                batch.settled(result.outcome());
                settle_outcome(
                    &mut Slot::new(msg),
                    result.outcome(),
                    subscription,
                    delivery,
                    C::build,
                )
                .await;
                // The continuation runs after this element is settled, on the tracked set so a
                // graceful shutdown drains it. At-most-once: a lost or panicking continuation
                // never redelivers the already-settled message.
                if let Some(after) = after {
                    delivery.spawn_after(after);
                }
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
    deliveries: Vec<Delivered>,
    settled: usize,
    _pending: PendingRecord,
}

#[cfg(feature = "testing")]
impl BatchLog {
    fn of<'m, M: IncomingMessage + 'm>(
        accepted: impl IntoIterator<Item = &'m M>,
        lent: LentValues,
    ) -> Self {
        let mut lent = lent.into_iter();
        Self {
            deliveries: accepted
                .into_iter()
                .map(|msg| Delivered {
                    raw: bytes::Bytes::copy_from_slice(msg.payload()),
                    value: lent.next(),
                    settle: None,
                })
                .collect(),
            settled: 0,
            _pending: PendingRecord::new(),
        }
    }

    fn settled(&mut self, status: HandlerResult) {
        if let Some(one) = self.deliveries.get_mut(self.settled) {
            one.settle = Some(status);
        }
        self.settled += 1;
    }

    fn record(self, subscription: &str) {
        coordinator::record_batch(subscription, self.deliveries);
    }
}
