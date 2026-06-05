//! Wiring: collect the [`orders`](crate::orders) handlers into one [`Router`].
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when [`main`](crate::main) mounts it with `include_router`.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Router, RouterDef, TypedPublisher};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the broker's publisher
/// with the default codec, and `include_publishing` reuses that codec to decode the order.
/// `on_cancel` has no reply, so it is mounted with `include` (also the default codec). The router is
/// a consuming builder, so the calls chain; the registration list is opaque, hence `impl RouterDef`.
///
/// `use<>` opts out of capturing the `broker` borrow: the router owns its publisher (Arc-backed), so
/// it does not borrow the broker, and the caller can still mutate the scope to mount it.
pub(crate) fn orders(broker: &MemoryBroker) -> impl RouterDef<MemoryBroker> + use<> {
    let confirmations = TypedPublisher::new(broker.publisher());

    Router::new()
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}
