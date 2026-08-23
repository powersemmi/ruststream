//! The tutorial's service at step 3: one handler mounted straight on the broker scope, before
//! the reply of step 4 and the router of step 5 arrive.
//!
//! ```text
//! cargo run --example tutorial_first_app --features macros,memory,json,asyncapi -- run
//! ```

// Step 3 mounts `handle` alone; the module's reply types stay unused until step 4.
#[allow(dead_code)]
// --8<-- [start:app]
mod orders;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

use crate::orders::handle;

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
// --8<-- [end:app]
