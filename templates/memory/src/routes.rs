//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - a router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{Router, RouterDef, TypedPublisher};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the broker's publish
/// policy with the default codec, reused to decode the order. The reply wiring is pure
/// declaration: the runtime pairs the policy with the connected broker at startup, so the router
/// borrows no broker. `on_cancel` has no reply, so it is mounted with `include` (also the default
/// codec). The router is a consuming builder, so the calls chain; the registration list is opaque,
/// hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<MemoryBroker> {
    let confirmations = TypedPublisher::new(MemoryPublish);

    Router::new()
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}
