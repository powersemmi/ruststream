//! The state of the service under test, against services written without the `macros` feature.
//! The test bodies are the ones of `examples/testing_service_state.rs`: the harness reports the
//! same state whichever way the service was declared.
//!
//! ```text
//! cargo test --example manual_testing_service_state --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

mod run_result {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::runtime::RustStreamError;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:run-result]
    struct Fulfil;

    impl Handle<Order> for Fulfil {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            assert_ne!(order.id, 0, "an order without an id reached fulfilment");
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Fulfil).build());
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde_json::json;

    use super::Order;

    // --8<-- [start:assert-running]
    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let _ = order.id;
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::runtime::PublishError;
    use ruststream::testing::{TestApp, TestError};

    use super::Order;

    // --8<-- [start:assert-shut-down]
    struct Fulfil;

    impl Handle<Order> for Fulfil {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            assert_ne!(order.id, 0, "an order without an id reached fulfilment");
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Fulfil).build());
        })
    }

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

fn main() {
    println!("the scenarios are tests: cargo test --example manual_testing_service_state");
}
