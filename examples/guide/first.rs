//! The shop as the first chapter of the "Build a service" guide leaves it: one subscriber on
//! the in-memory broker. The rest of the guide grows it into the service of `main.rs`.
//!
//! ```text
//! cargo run --example guide_first --features testing,macros,memory,json,logging -- run
//! cargo test --example guide_first --features testing,macros,memory,json
//! ```

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:order]
/// An order placed in the storefront.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.placed")]
struct Order {
    id: u64,
    customer: String,
}
// --8<-- [end:order]

// --8<-- [start:accept]
/// Takes every placed order; for now it only logs it.
#[subscriber("orders.placed")]
async fn accept(order: &Order) -> HandlerOutcome {
    tracing::info!(order = order.id, customer = %order.customer, "order accepted");
    HandlerOutcome::ack()
}
// --8<-- [end:accept]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(accept);
    })
}
// --8<-- [end:app]

// The generated `main` runs the builder; this test runs the same builder in process.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn a_placed_order_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            customer: "ada@example.com".to_owned(),
        };
        tb.message(&order).publish().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders.placed")
            .assert_called_once()
            .with(&order)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
