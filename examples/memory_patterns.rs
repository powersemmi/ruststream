//! Pattern subscriptions on the in-memory broker: a subscription to `orders.*` reads every name
//! the pattern matches, and the broker's routing rule decides who receives a publish that several
//! subscriptions match.
//!
//! ```text
//! cargo run --example memory_patterns --features testing,macros,memory,json -- run
//! cargo test --example memory_patterns --features testing,macros,memory,json
//! ```

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:app]
#[subscriber("orders.eu")]
async fn europe(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "european order");
    HandlerOutcome::ack()
}

/// Reads every `orders.<region>` name.
#[subscriber(MemoryPattern::new("orders.*"))]
async fn other_regions(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order from another region");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // `orders.eu` goes to `europe` alone; the pattern takes the names nobody else reads.
    let broker = MemoryBroker::new().routing(Routing::MostSpecific);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(europe);
        b.include(other_regions);
    })
}
// --8<-- [end:app]

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
