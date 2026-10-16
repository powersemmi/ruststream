//! A subscriber: an `async fn` over a reference to the message, declared with `#[subscriber]` and
//! mounted on a broker with `include`. The scenario is a test.
//!
//! ```text
//! cargo test --example subscriber_handler --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

mod handler {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:handler]
    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }
    // --8<-- [end:handler]

    #[tokio::test]
    async fn the_handler_takes_each_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
