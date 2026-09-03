//! Order-lifecycle handlers: confirming a new order (a publishing handler that replies and handles
//! transient failures) and cancelling one (a plain handler that retries on a blip).
//!
//! Both read the shared [`Repository`](crate::domain::Repository) from the per-delivery
//! [`Context`], inserted once by the startup hook in [`main`](crate::main).

use ruststream::memory::MemorySource;
use ruststream::runtime::HandlerOutcome;
use ruststream::subscriber;

use crate::domain::{Cancellation, Confirmation, Order, Repository};

/// Confirms an order and replies on `confirmations`.
///
/// Bound through the macro's descriptor form, `MemorySource::new("orders")`, rather than a bare
/// name - the slot where a real broker takes its own descriptor (a NATS `SubscribeOptions`, say).
/// Returning `Result<Confirmation, HandlerOutcome>` keeps control of the acknowledgement: `Ok`
/// publishes the reply and acks, while `Err` publishes nothing and hands the dispatcher a
/// [`HandlerOutcome`] - here, retry on a transient store error and drop on a permanent one. The
/// `publish("confirmations")` clause names the reply channel; its publisher is wired in
/// [`routes`](crate::routes).
// --8<-- [start:descriptor]
#[subscriber(MemorySource::new("orders"), publish("confirmations"))]
pub(crate) async fn confirm(
    order: &Order,
    ctx: &mut Context<'_, (), Repository>,
) -> Result<Confirmation, HandlerOutcome> {
    let repo = ctx.state();
    tracing::debug!(
        order = order.id,
        customer = %order.customer,
        item = %order.item,
        "confirming order"
    );
    match repo.record_order(order.id).await {
        Ok(()) => Ok(Confirmation {
            order_id: order.id,
            accepted: order.quantity > 0,
        }),
        Err(e) if e.is_transient() => {
            tracing::warn!(order = order.id, "store busy, asking for redelivery");
            Err(HandlerOutcome::retry())
        }
        Err(e) => {
            tracing::error!(order = order.id, error = %e, "dropping order");
            Err(HandlerOutcome::drop())
        }
    }
}
// --8<-- [end:descriptor]

/// Cancels an order, bound by plain name. No reply, so it returns a plain [`HandlerOutcome`]: retry
/// a transient store error, ack everything else (an unknown order is nothing to undo).
// --8<-- [start:retry]
#[subscriber("cancellations")]
pub(crate) async fn on_cancel(
    cancel: &Cancellation,
    ctx: &mut Context<'_, (), Repository>,
) -> HandlerOutcome {
    let repo = ctx.state();
    match repo.cancel(cancel.order_id).await {
        // A transient blip is worth a redelivery; an unknown order (or success) is nothing to undo.
        Err(e) if e.is_transient() => HandlerOutcome::retry(),
        _ => HandlerOutcome::ack(),
    }
}
// --8<-- [end:retry]
