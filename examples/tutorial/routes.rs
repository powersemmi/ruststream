//! The tutorial's router: collects the [`orders`](crate::orders) handlers into one group.

// --8<-- [start:routes]
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Router, RouterDef, TypedPublisher};

use crate::orders;

pub(crate) fn orders(broker: &MemoryBroker) -> impl RouterDef<MemoryBroker> + use<> {
    let replies = TypedPublisher::new(broker.publisher());
    Router::new()
        .include_publishing(orders::confirm, replies)
        .include(orders::handle)
}
// --8<-- [end:routes]
