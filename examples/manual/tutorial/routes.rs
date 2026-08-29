//! The tutorial's router without the `macros` feature: the same group, over the value
//! constructors.

// --8<-- [start:routes]
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;

use crate::orders::{Confirm, Handle};

// Each handler is bound to its subject where it is mounted; the reply form names its
// destination and the publisher it leaves through. The publisher wiring is still a publish
// policy - pure declaration, so the router needs no broker at all.
pub(crate) fn orders() -> impl RouterDef<MemoryBroker> {
    let replies = TypedPublisher::new(MemoryPublish);
    Router::new()
        .include(subscriber("orders", Handle).documented())
        .include(replying("orders", Confirm).to("confirmations").documented())
        .publisher(replies)
}
// --8<-- [end:routes]
