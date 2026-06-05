//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition that `routes` collects into a `Router`. `confirm` consumes `orders` and replies on
//! `confirmations`; `on_cancel` handles `cancellations` with no reply.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order placed on the `orders` subject.
///
/// `JsonSchema` lets `asyncapi gen` emit this payload's schema into the generated document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Order {
    pub id: u64,
    pub item: String,
    pub quantity: u32,
}

/// The reply published to `confirmations` for each order.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Confirms an incoming order and publishes a `Confirmation` to `confirmations`.
///
/// The return value is the reply: the `publish("confirmations")` clause makes the runtime encode it
/// and send it through the publisher wired in `routes`.
#[subscriber("orders", publish("confirmations"))]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Logs cancellations, bound by plain name. No reply, so it returns a plain `HandlerResult`.
#[subscriber("cancellations")]
pub async fn on_cancel(order: &Order) -> HandlerResult {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerResult::Ack
}
