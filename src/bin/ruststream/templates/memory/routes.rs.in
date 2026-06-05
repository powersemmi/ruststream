//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Router, RouterDef, TypedPublisher};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the broker's publisher
/// with the default codec, reused to decode the order. `on_cancel` has no reply, so it is mounted
/// with `include` (also the default codec). The router is a consuming builder, so the calls chain;
/// the registration list is opaque, hence `impl RouterDef`. `use<>` opts out of borrowing `broker`
/// (the router owns its Arc-backed publisher), so `main` can still mutate the scope to mount it.
pub fn orders(broker: &MemoryBroker) -> impl RouterDef<MemoryBroker> + use<> {
    let confirmations = TypedPublisher::new(broker.publisher());

    Router::new()
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}
