//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - a router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::memory::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply; `.out(Reply, Publish)` names the position the reply
/// leaves through and the broker's publish policy, and the reply encodes with the default codec
/// unless the chain names one (`.codec(..)`). The reply wiring is pure declaration: the runtime
/// pairs the policy with the connected broker at startup, so the router borrows no broker.
/// `on_cancel` has no reply, so its `include` registers on its own. The router is a consuming
/// builder, so a registration that carries a publish position closes with `.build()` and the calls
/// chain; the registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<MemoryBroker> {
    Router::new()
        .include(orders::confirm)
        .out(Reply, Publish)
        .build()
        .include(orders::on_cancel)
}
