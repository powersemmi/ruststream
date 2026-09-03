//! The Getting-started tutorial service without the `macros` feature: the same two handlers in
//! [`orders`], the same router in [`routes`], and a hand-written entry point here.
//!
//! ```text
//! cargo run --example manual_tutorial --no-default-features --features memory,json,asyncapi
//! ```

// --8<-- [start:main]
mod orders;
mod routes;

use std::error::Error;

use ruststream::memory::prelude::*;

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders();
        b.include_router(router);
    })
}

// What `#[ruststream::app]` wraps around the builder: the runtime entry point. Its CLI
// (`run`, `asyncapi gen`) is what a hand-written `main` gives up.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:main]
