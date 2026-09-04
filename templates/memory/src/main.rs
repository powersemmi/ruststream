//! {{project-name}} - a RustStream service.
//!
//! Handlers live in `orders`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.

mod orders;
mod routes;

use ruststream::memory::prelude::*;

/// Builds the service: one in-memory broker with the orders router mounted.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(routes::orders());
        })
}
