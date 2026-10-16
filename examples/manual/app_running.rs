//! The services of `examples/app_running.rs` written without the `macros` feature. The test
//! bodies are the same: `start`, `run_until` and the handle behave the same whichever way the
//! service was declared.
//!
//! ```text
//! cargo test --example manual_app_running --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
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

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    order_id: u64,
}

// `#[outgoing(name = "receipts")]` by hand: the type fixes its destination.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

mod start {
    use std::error::Error;
    use std::future::{Future, ready};
    use std::pin::pin;
    use std::time::Duration;

    use futures::StreamExt;
    use ruststream::Subscriber;
    use ruststream::memory::MemorySubscriber;
    use ruststream::memory::prelude::*;
    use ruststream::runtime::HealthState;
    use tokio::time::timeout;

    use super::{Order, Receipt};

    // --8<-- [start:start]
    struct IssueReceipt;

    impl Handle<Order, Receipt> for IssueReceipt {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            ready(Ok(Receipt { order_id: order.id }))
        }
    }

    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(subscriber("orders", IssueReceipt).reply().build())
                .out_reply(Publish);
        })
    }

    /// The next receipt a downstream reader sees, or `None` after five seconds.
    async fn next_receipt(receipts: &mut MemorySubscriber) -> Option<Receipt> {
        let mut stream = pin!(receipts.stream());
        match timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(delivery))) => serde_json::from_slice(delivery.payload()).ok(),
            _ => None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_runs_the_service_beside_the_caller() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let mut receipts = broker.subscribe("receipts");
        let orders = broker.publisher();

        // `start` returns once every subscription is open.
        let running = app(broker).start().await?;
        let health = running.health();
        assert_eq!(health.state(), HealthState::Running);

        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;
        assert_eq!(
            next_receipt(&mut receipts).await,
            Some(Receipt { order_id: 7 })
        );

        running.shutdown().await?;
        assert_eq!(health.state(), HealthState::Stopped);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_until_stops_when_the_future_resolves() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let mut receipts = broker.subscribe("receipts");
        let orders = broker.publisher();

        // The future runs once the service has started; the service stops when it resolves.
        let mut receipt = None;
        let first_receipt = async {
            let sent = orders
                .message(&Order { id: 7 })
                .to("orders")
                .publish()
                .await;
            if sent.is_ok() {
                receipt = next_receipt(&mut receipts).await;
            }
        };
        app(broker).run_until(first_receipt).await?;

        assert_eq!(receipt, Some(Receipt { order_id: 7 }));
        Ok(())
    }
    // --8<-- [end:start]
}

fn main() {
    println!("the scenarios are tests: cargo test --example manual_app_running");
}
