//! The tutorial's message types and handlers.

// --8<-- [start:order]
use ruststream::runtime::HandlerOutcome;
use ruststream::schemars::JsonSchema;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) quantity: u32,
}

#[subscriber("orders")]
pub(crate) async fn handle(order: &Order) -> HandlerOutcome {
    println!("order {} x{}", order.id, order.quantity);
    HandlerOutcome::ack()
}
// --8<-- [end:order]

// --8<-- [start:confirm]
/// The service's answer to an order.
#[derive(Debug, Serialize, JsonSchema)]
pub(crate) struct Confirmation {
    pub(crate) id: u64,
    pub(crate) accepted: bool,
}

#[subscriber("orders", publish("confirmations"))]
pub(crate) async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}
// --8<-- [end:confirm]
