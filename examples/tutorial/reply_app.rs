//! The tutorial's service at step 4: the reply handler mounted next to the plain one, still
//! without the router of step 5.
//!
//! ```text
//! cargo run --example tutorial_reply_app --features macros,memory,json,asyncapi -- run
//! ```

mod orders;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

// --8<-- [start:reply]
use crate::orders::{confirm, handle};

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
        b.include(confirm);
    })
}
// --8<-- [end:reply]
