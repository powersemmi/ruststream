//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that [`routes`](crate::routes) collects into a
//! [`Router`](ruststream::runtime::Router). `confirm` binds through the broker-specific
//! [`OrdersStream`](crate::stream::OrdersStream) descriptor; `on_cancel` binds by plain name.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

use crate::stream::OrdersStream;

/// An order placed on the `orders` channel.
#[derive(Debug, Deserialize)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) item: String,
    pub(crate) quantity: u32,
}

/// The reply published to `confirmations` for each order.
#[derive(Debug, Serialize)]
pub(crate) struct Confirmation {
    pub(crate) id: u64,
    pub(crate) accepted: bool,
}

/// Confirms an incoming order and publishes a [`Confirmation`] to `confirmations`.
///
/// Bound through the broker-specific [`OrdersStream`] descriptor (consumer group `workers`) rather
/// than a bare name. The return value is the reply: the `publish("confirmations")` clause makes the
/// runtime encode it and send it through the publisher wired in [`routes`](crate::routes).
#[subscriber(OrdersStream::new("orders", "workers"), publish("confirmations"))]
pub(crate) async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Logs cancellations, bound by plain name. No reply, so it returns a plain [`HandlerResult`].
#[subscriber("cancellations")]
pub(crate) async fn on_cancel(order: &Order) -> HandlerResult {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerResult::Ack
}
