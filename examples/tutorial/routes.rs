//! The tutorial's router: collects the [`orders`](crate::orders) handlers into one group.

// --8<-- [start:routes]
use ruststream::memory::prelude::*;

use crate::orders;

// The reply wiring is a publish policy: pure declaration, so the router needs no broker at all.
pub(crate) fn orders() -> impl RouterDef<MemoryBroker> {
    Router::new()
        .include(orders::handle)
        .include(orders::confirm)
        .out(Reply, Publish)
        .build()
}
// --8<-- [end:routes]
