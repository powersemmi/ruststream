//! Wiring: collect the [`orders`](crate::orders) handlers into one [`Router`].
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when [`main`](crate::main) mounts it with `include_router`.

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Router, TypedPublisher};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to `confirmations`) plus a plain one.
///
/// `confirm` needs a publisher for its reply; we hand it the broker's publisher with a JSON reply
/// codec. `on_cancel` has no reply, so it is mounted with `include`.
pub(crate) fn orders(broker: &MemoryBroker) -> Router<MemoryBroker> {
    let confirmations = TypedPublisher::new(broker.publisher(), JsonCodec);

    let mut router = Router::new();
    router.include_publishing(orders::confirm, JsonCodec, confirmations);
    router.include(orders::on_cancel, JsonCodec);
    router
}
