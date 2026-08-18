//! The tutorial's router: collects the [`orders`](crate::orders) handlers into one group.

// --8<-- [start:routes]
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{Router, RouterDef, TypedPublisher};

use crate::orders;

// The reply wiring is a publish policy: pure declaration, so the router needs no broker at all.
pub(crate) fn orders() -> impl RouterDef<MemoryBroker> {
    let replies = TypedPublisher::new(MemoryPublish);
    Router::new()
        .include(orders::confirm)
        .publisher(replies)
        .include(orders::handle)
}
// --8<-- [end:routes]
