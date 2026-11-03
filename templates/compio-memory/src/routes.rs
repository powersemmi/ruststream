//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.

use ruststream::memory::local::LocalMemoryPublish;
use ruststream::runtime::{Router, RouterDef};

use crate::{Bus, orders};

/// Builds the orders router: a handler that replies to `confirmations`, and one that audits.
///
/// `.out_reply(LocalMemoryPublish)` names the position `confirm`'s reply leaves through and the
/// bus's publish policy; the runtime pairs it with the connected bus at startup. `audit` has no
/// reply, so its `include` registers on its own.
pub fn orders() -> impl RouterDef<Bus> {
    Router::new()
        .include(orders::confirm)
        .out_reply(LocalMemoryPublish)
        .build()
        .include(orders::audit)
}
