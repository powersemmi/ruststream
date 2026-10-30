//! The service of `examples/app_macro.rs` without the `macros` feature: `main` is written by hand
//! and hands the builder to `run_main`, which is what `#[ruststream::app]` expands to.
//!
//! ```text
//! cargo run --example manual_app_macro --no-default-features --features testing,memory,json,asyncapi -- asyncapi gen
//! cargo test --example manual_app_macro --no-default-features --features testing,memory,json
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:app]
struct Accept;

impl Handle<Order> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "accepted");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Accept).build())
    })
}

// What `#[ruststream::app]` writes: the command line (`run`, `asyncapi gen`) on a multi-threaded
// Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

// `main` runs the builder; this test runs the same builder on the in-process harness.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn the_builder_mounts_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        orders
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        Ok(())
    }
}
