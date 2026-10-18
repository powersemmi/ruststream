//! The service of `examples/memory_patterns.rs` without the `macros` feature: the pattern is the
//! source the `subscriber` constructor takes.
//!
//! ```text
//! cargo run --example manual_memory_patterns --no-default-features --features testing,memory,json -- run
//! cargo test --example manual_memory_patterns --no-default-features --features testing,memory,json
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
struct Europe;

impl Handle<Order> for Europe {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "european order");
        ready(Ok(()))
    }
}

struct OtherRegions;

impl Handle<Order> for OtherRegions {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order from another region");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // `orders.eu` goes to `Europe` alone; the pattern takes the names nobody else reads.
    let broker = MemoryBroker::new().routing(Routing::MostSpecific);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders.eu", Europe).build());
        // Reads every `orders.<region>` name.
        b.include(subscriber(MemoryPattern::new("orders.*"), OtherRegions).build());
    })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn the_pattern_receives_only_the_names_without_a_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 1 })
            .to("orders.eu")
            .publish()
            .await?;
        orders
            .message(&Order { id: 2 })
            .to("orders.us")
            .publish()
            .await?;

        orders
            .subscriber("orders.eu")
            .assert_called_once()
            .with(&Order { id: 1 });
        orders
            .subscriber("orders.*")
            .assert_called_once()
            .with(&Order { id: 2 });
        Ok(())
    }
}
