//! The tutorial's message types and handlers.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

// --8<-- [start:order]
#[derive(Debug, Deserialize)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) quantity: u32,
}

#[subscriber("orders")]
pub(crate) async fn handle(order: &Order) -> HandlerResult {
    println!("order {} x{}", order.id, order.quantity);
    HandlerResult::Ack
}
// --8<-- [end:order]

// --8<-- [start:confirm]
#[derive(Debug, Serialize)]
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
