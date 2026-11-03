//! Another broker's publisher: a handler consumes from one broker and its reply leaves through
//! another, named by a token the target broker minted before registration. The scenario is a
//! test.
//!
//! ```text
//! cargo test --example cross_broker --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.mirrored")]
struct Mirrored {
    id: u64,
}

#[subscriber("orders", reply)]
async fn mirror(order: &Order) -> Mirrored {
    Mirrored { id: order.id }
}

#[subscriber("orders.mirrored")]
async fn record(order: &Mirrored) -> HandlerOutcome {
    tracing::info!(order.id, "mirrored order recorded");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
fn app() -> impl App {
    // The target broker is wrapped first, so its token exists before either scope is built.
    let west = MemoryBroker::new().bindable();
    let to_west = west.bind(Publish);
    RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .with_broker_labeled("east", MemoryBroker::new(), |b| {
            // Consumed in the east, answered in the west.
            b.include(mirror).out_reply(to_west)
        })
        .with_broker_labeled("west", west, |b| b.include(record))
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Mirrored, Order, app};

    #[tokio::test]
    async fn an_order_from_the_east_reaches_the_west() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("east")
            .message(&Order { id: 5 })
            .to("orders")
            .publish()
            .await?;

        tb.broker_named("west")
            .published::<Mirrored>("orders.mirrored")
            .assert_called_once()
            .with(&Mirrored { id: 5 });
        tb.broker_named("west")
            .subscriber("orders.mirrored")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker_named("east")
            .published::<Mirrored>("orders.mirrored")
            .assert_not_called();
        Ok(())
    }
}
