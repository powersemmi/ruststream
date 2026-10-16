//! A service started in the background with `start` and stopped by its handle, and the same
//! service run with `run_until` until a future of the caller resolves. Each scenario is a test.
//!
//! ```text
//! cargo test --example app_running --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

mod start {
    use std::error::Error;
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
    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(issue_receipt).out_reply(Publish);
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
    println!("the scenarios are tests: cargo test --example app_running");
}
