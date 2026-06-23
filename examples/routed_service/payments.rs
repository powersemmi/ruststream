//! Payment handlers: charging a payment across keyed worker lanes, and settling a batch of cleared
//! payments in one transaction.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use std::time::Duration;

use crate::domain::{Clearing, Payment, Repository, Settlement};

/// Charges a payment. `workers(8, by_key)` runs up to eight charges at once, but keyed by the
/// message's partition key so payments for one customer keep their arrival order while different
/// customers proceed in parallel. A transient gateway error asks for delayed redelivery rather than
/// an immediate retry, so a flaky downstream is not hammered.
// --8<-- [start:workers]
#[subscriber("payments", workers(8, by_key))]
pub(crate) async fn process_payment(
    payment: &Payment,
    ctx: &mut Context<'_, (), Repository>,
) -> HandlerResult {
    let repo = ctx.state();
    tracing::debug!(order = payment.order_id, customer = %payment.customer, "charging payment");
    if repo
        .charge(payment.order_id, payment.amount_cents)
        .await
        .is_err()
    {
        return HandlerResult::retry_after(Duration::from_secs(2));
    }
    HandlerResult::Ack
}
// --8<-- [end:workers]

/// Settles a page of cleared payments and publishes the settlements on `settlements`. Mounted with
/// a transactional publisher in [`routes`](crate::routes), so the whole page becomes visible
/// atomically on commit. The batch contract guarantees a non-empty page, so the handler maps it
/// straight to replies; returning the bare `Vec` publishes them all and acks the batch.
// --8<-- [start:batch]
// This handler ignores the app state, so it omits the `Context` parameter and stays generic over
// the state; it still mounts alongside the stateful `process_payment` handler on the same router.
#[subscriber(batch("clearings"), publish("settlements"))]
pub(crate) async fn settle(clearings: &[Clearing]) -> Vec<Settlement> {
    clearings
        .iter()
        .map(|c| Settlement {
            order_id: c.order_id,
            amount_cents: c.amount_cents,
        })
        .collect()
}
// --8<-- [end:batch]
