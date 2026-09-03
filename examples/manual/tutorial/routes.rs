//! The tutorial's router without the `macros` feature: the same group, over the one value
//! constructor.

// --8<-- [start:routes]
use ruststream::memory::prelude::*;

use crate::orders::{Confirm, Receive};

// Each handler is bound to its subject where it is mounted; the reply chain names its
// destination and the publisher it leaves through. The publisher wiring is still a publish
// policy - pure declaration, so the router needs no broker at all.
pub(crate) fn orders() -> impl RouterDef<MemoryBroker> {
    Router::new()
        .include(subscriber("orders", Receive).build())
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .publisher(Publish)
                .build(),
        )
}
// --8<-- [end:routes]
