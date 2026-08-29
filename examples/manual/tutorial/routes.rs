//! The tutorial's router without the `macros` feature: the same group, over hand-written
//! definitions.

// --8<-- [start:routes]
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::{HandlerMetadata, RouterDef, TypedPublisher, typed};

use crate::orders::{Confirm, Handle, Order};

// A plain handler is registered with the subject and the codec; only the reply form needs
// `include`, for the publisher it attaches. That wiring is still a publish policy - pure
// declaration, so the router needs no broker at all.
pub(crate) fn orders() -> impl RouterDef<MemoryBroker> {
    let replies = TypedPublisher::new(MemoryPublish);
    Router::new()
        .subscribe(
            Name::new("orders"),
            typed(JsonCodec, Handle),
            HandlerMetadata::typed::<Order>("orders"),
        )
        .include(Confirm)
        .publisher(replies)
}
// --8<-- [end:routes]
