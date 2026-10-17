//! What a subscriber does when its handler panics or a payload cannot decode: the defaults, and
//! `on_failure(panic = .., decode = ..)` per subscriber. Each scenario is a test.
//!
//! ```text
//! cargo test --example failure_policy --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

mod defaults {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde_json::json;

    use super::Order;

    // --8<-- [start:defaults]
    /// No clause: a payload that cannot decode is dropped, and a panic stops the service.
    #[subscriber("orders")]
    async fn fulfil(order: &Order) -> HandlerOutcome {
        assert_ne!(order.id, 0, "an order without an id reached fulfilment");
        HandlerOutcome::ack()
    }
    // --8<-- [end:defaults]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(fulfil);
        })
    }

    #[tokio::test]
    async fn bad_input_is_dropped_and_a_bug_stops_the_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.publish("orders", &json!({ "id": "seven" })).await?;
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        tb.assert_running();

        tb.message(&Order { id: 0 }).publish().await?;
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(2)
            .panicked();
        tb.assert_shut_down();
        Ok(())
    }
}

mod skip {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};
    use serde_json::json;

    use super::Order;

    // --8<-- [start:skip]
    /// An audit trail moves past anything it cannot process: both failures ack the delivery.
    #[subscriber("orders", on_failure(panic = skip, decode = skip))]
    async fn audit(order: &Order) -> HandlerOutcome {
        assert_ne!(order.id, 0, "an order without an id reached the audit");
        HandlerOutcome::ack()
    }
    // --8<-- [end:skip]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(audit);
        })
    }

    #[tokio::test]
    async fn the_audit_keeps_going_past_failures() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.publish("orders", &json!({ "id": "seven" })).await?;
        tb.message(&Order { id: 0 }).publish().await?;
        tb.message(&Order { id: 7 }).publish().await?;

        assert_eq!(
            tb.broker::<MemoryBroker>().subscriber("orders").outcomes(),
            [Outcome::DecodeFailed, Outcome::Panicked, Outcome::Ack],
        );
        tb.assert_running();
        Ok(())
    }
}

mod tuned {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde_json::json;

    use super::Order;

    // --8<-- [start:tuned]
    const SCHEMA_ROLLOUT: Duration = Duration::from_secs(60);

    /// Producers roll a schema change out first: a payload this version cannot read yet comes
    /// back in a minute instead of being dropped. A panic still stops the service.
    #[subscriber("orders", on_failure(decode = retry_after(SCHEMA_ROLLOUT)))]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }
    // --8<-- [end:tuned]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreadable_payload_comes_back_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.publish("orders", &json!({ "id": "seven" })).await?;
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_last_failed_to_decode();

        tb.advance(SCHEMA_ROLLOUT).await?;
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(2)
            .assert_last_failed_to_decode();
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
