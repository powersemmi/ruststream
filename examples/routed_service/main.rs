//! A multi-file RustStream service: macro handlers in [`orders`], wiring in [`routes`], and a
//! `#[ruststream::app]` entry point here that mounts the router.
//!
//! Run it with `cargo run --example routed_service --features macros,memory -- run`, or generate
//! its AsyncAPI document with `... --features macros,memory,asyncapi -- asyncapi gen`.

mod orders;
mod routes;
mod stream;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

/// Builds the service. `#[ruststream::app]` generates `main`, so there is no runtime boilerplate.
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
}
