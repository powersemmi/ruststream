//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! `confirm` consumes `orders` with four workers, which share the available cores, core 0
//! included, and replies on `confirmations`. `audit` reads the same orders on core 0, where the
//! bus receives.

use ruststream::memory::local::LocalMemorySource;
use ruststream::runtime::compio::Core;
use ruststream::runtime::{Ctx, HandlerOutcome};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// An order placed on the `orders` channel.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// The reply published to `confirmations` for each order.
///
/// `Outgoing` is what declares it a message this service sends, and `name` is where it goes.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
    /// The core whose worker confirmed the order.
    pub core: usize,
}

/// Confirms an incoming order on one of four workers and publishes a `Confirmation`.
///
/// The return value is the reply: the `reply` clause makes the runtime encode it and send it
/// through the publisher wired in `routes`. `Ctx<Core>` reads the core the worker runs on.
#[subscriber(LocalMemorySource::new("orders"), workers(4), reply)]
pub async fn confirm(order: &Order, Ctx(core): Ctx<Core>) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
        core: core.index(),
    }
}

/// Audits every order on core 0. No reply, so it returns a plain `HandlerOutcome`.
#[subscriber(LocalMemorySource::new("orders"))]
pub async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.quantity, "order audited");
    HandlerOutcome::ack()
}
