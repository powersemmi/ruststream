//! A service on the in-memory broker: two subscriptions to one name each receive every message
//! published to it, and a reply goes back over the same bus.
//!
//! ```text
//! cargo run --example memory_broker --features testing,macros,memory,json -- run
//! cargo test --example memory_broker --features testing,macros,memory,json
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream::memory::prelude::*;

#[subscriber("orders", reply("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber("orders")]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "audited");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(confirm).include(audit))
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Confirmation {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn every_subscription_of_the_name_receives_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        // Two subscriptions to `orders`: one delivery each.
        orders
            .subscriber("orders")
            .assert_called(2)
            .with(&Order { id: 7 });
        orders
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 7 });
        Ok(())
    }
}
