//! The landing-page example written without the `macros` feature: the handler is a named type
//! with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is the one
//! `#[ruststream::app]` would generate.
//!
//! ```text
//! cargo run --example manual_quickstart --no-default-features --features memory,json,logging -- run
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The handler: `#[subscriber("orders")]` generates this type and this impl. Every axis of the
/// form - the reply, the injections, the broker context, the application state - is a defaulted
/// parameter of `Handle`, so a plain body names none of them.
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "got order");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Receive).build());
    })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]
