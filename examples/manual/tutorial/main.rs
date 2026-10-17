//! The Getting-started tutorial service without the `macros` feature: the same two handlers in
//! [`orders`], the same router in [`routes`], and a hand-written entry point here.
//!
//! ```text
//! cargo run --example manual_tutorial --no-default-features --features memory,json,asyncapi,logging -- run
//! ```

// --8<-- [start:main]
mod orders;
mod routes;

use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders();
        b.include_router(router);
    })
}

// What `#[ruststream::app]` writes around the builder: the `run` and `asyncapi gen` commands.
fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:main]
