//! Settings at the mount site: a subscriber whose attribute leaves the name, the concurrency and
//! the failure policy open gets them where it is mounted. The scenario is a test.
//!
//! ```text
//! cargo test --example subscriber_settings --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// Bytes published as themselves: how the test injects a payload that does not decode.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

mod settings {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Wire};

    // --8<-- [start:settings]
    /// Everything is left to the mount site: the name, the pool, the failure policy.
    #[subscriber]
    async fn bill(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "billed");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(
                bill.name("orders")
                    .workers(nonzero!(4))
                    // A payload that does not decode is acked and skipped, not dropped.
                    .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)),
            )
        })
    }
    // --8<-- [end:settings]

    #[tokio::test]
    async fn a_malformed_order_is_skipped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        // Bytes that are no JSON order.
        broker
            .message(&Wire(b"not json".to_vec()))
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .assert_last_failed_to_decode();
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
