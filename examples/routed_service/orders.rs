//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that [`routes`](crate::routes) collects into a
//! [`Router`](ruststream::runtime::Router). `confirm` binds through the memory broker's
//! [`MemorySource`](ruststream::memory::MemorySource) descriptor (the macro's descriptor form);
//! `on_cancel` binds by plain name.

use ruststream::memory::MemorySource;
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

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
/// Bound through the macro's descriptor form, `MemorySource::new("orders")`, rather than a bare
/// name - the slot where a real broker takes its own descriptor (a NATS `SubscribeOptions`, say).
/// The return value is the reply: the `publish("confirmations")` clause makes the runtime encode it
/// and send it through the publisher wired in [`routes`](crate::routes).
#[subscriber(MemorySource::new("orders"), publish("confirmations"))]
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
