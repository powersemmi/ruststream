//! The state of the service under test: whether it is still running, and what its run would have
//! returned. Every scenario is a test of its own, the way a service writes it.
//!
//! ```text
//! cargo test --example testing_service_state --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

mod run_result {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::runtime::RustStreamError;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn fulfil(order: &Order) -> HandlerOutcome {
        assert_ne!(order.id, 0, "an order without an id reached fulfilment");
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(fulfil);
        })
    }

    // --8<-- [start:run-result]
    #[tokio::test]
    async fn a_panic_ends_the_run_with_an_error() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order { id: 7 }).publish().await?;
        tb.run_result()?;

        tb.message(&Order { id: 0 }).publish().await?;
        assert!(matches!(tb.run_result(), Err(RustStreamError::Dispatch(_))));
        Ok(())
    }
    // --8<-- [end:run-result]
}

mod assert_running {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde_json::json;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }

    // --8<-- [start:assert-running]
    #[tokio::test]
    async fn an_undecodable_order_does_not_stop_the_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.publish("orders", &json!({ "id": "seven" })).await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        tb.assert_running();
        Ok(())
    }
    // --8<-- [end:assert-running]
}

mod assert_shut_down {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::runtime::PublishError;
    use ruststream::testing::{TestApp, TestError};

    use super::Order;

    #[subscriber("orders")]
    async fn fulfil(order: &Order) -> HandlerOutcome {
        assert_ne!(order.id, 0, "an order without an id reached fulfilment");
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(fulfil);
        })
    }

    // --8<-- [start:assert-shut-down]
    #[tokio::test]
    async fn a_panic_shuts_the_service_down() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order { id: 0 }).publish().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .panicked();
        tb.assert_shut_down();
        assert!(matches!(
            tb.message(&Order { id: 7 }).publish().await,
            Err(PublishError::Publish(TestError::ShutDown))
        ));
        Ok(())
    }
    // --8<-- [end:assert-shut-down]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
