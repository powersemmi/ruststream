//! The tutorial's router without the `macros` feature: the same group, over the one value
//! constructor.

// --8<-- [start:routes]
use ruststream::memory::prelude::*;

use crate::orders::{Confirm, Receive};

// Each handler is bound to its subject where it is mounted; the definition says what it replies
// with and where, and the mount chain names who publishes it. The publisher wiring is still a
// publish policy - pure declaration, so the router needs no broker at all.
pub(crate) fn orders() -> impl RouterDef<MemoryBroker> {
    Router::new()
        .include(subscriber("orders", Receive).build())
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, Publish)
        .build()
}
// --8<-- [end:routes]
