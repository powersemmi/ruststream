//! The tutorial's router: collects the [`orders`](crate::orders) handlers into one group.

// --8<-- [start:routes]
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Router, TypedPublisher};

use crate::orders;

pub(crate) fn orders(broker: &MemoryBroker) -> Router<MemoryBroker> {
    let replies = TypedPublisher::new(broker.publisher());
    let mut router = Router::new();
    router.include_publishing(orders::confirm, replies);
    router.include(orders::handle);
    router
}
// --8<-- [end:routes]
