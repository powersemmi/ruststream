//! The Getting-started tutorial service, exactly as the docs build it: a message type and two
//! handlers in [`orders`], a router in [`routes`], and the `#[ruststream::app]` entry point here.
//!
//! ```text
//! cargo run --example tutorial --features macros,memory,json -- run
//! ```

// --8<-- [start:main]
mod orders;
mod routes;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders();
        b.include_router(router);
    })
}
// --8<-- [end:app]
// --8<-- [end:main]
