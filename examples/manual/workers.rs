//! The service of `examples/workers.rs` written without the `macros` feature: the pool is a
//! setting on the mount chain, `.workers(n)` or `.workers_by_key(n)` before `.build()`.
//!
//! ```text
//! cargo test --example manual_workers --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::time;

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

/// Stands in for a call to a payment provider: a second of waiting on I/O.
async fn charge(order: &Order) {
    time::sleep(Duration::from_secs(1)).await;
    tracing::info!(order.id, "charged");
}

/// The body both services mount: the pool is the mount site's word.
struct Bill;

impl Handle<Order> for Bill {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        charge(order).await;
        Ok(())
    }
}

mod pool {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tokio::time;
    use tokio::time::Instant;

    use super::{Bill, Order};

    // --8<-- [start:workers]
    /// Up to four orders are charged at once; the next one waits for a free worker.
    fn app() -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Bill).workers(nonzero!(4)).build());
        })
    }
    // --8<-- [end:workers]

    #[tokio::test]
    async fn four_orders_are_charged_together() -> Result<(), Box<dyn Error>> {
        time::pause();
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let orders: Vec<Order> = (1..=4).map(|id| Order { id }).collect();

        let started = Instant::now();
        for published in join_all(
            orders
                .iter()
                .map(|order| broker.message(order).to("orders").publish()),
        )
        .await
        {
            published?;
        }

        // Four charges of a second each, side by side: one second passed, not four.
        assert_eq!(started.elapsed().as_secs(), 1);
        broker
            .subscriber("orders")
            .assert_called(4)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

mod by_key {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::memory::PARTITION_KEY_HEADER;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tokio::time;
    use tokio::time::Instant;

    use super::{Bill, Order};

    // --8<-- [start:workers_by_key]
    /// Four lanes by the message's partition key: one customer's orders are charged in order,
    /// different customers' at once.
    fn app() -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(
                subscriber("orders", Bill)
                    .workers_by_key(nonzero!(4))
                    .build(),
            );
        })
    }
    // --8<-- [end:workers_by_key]

    #[tokio::test]
    async fn one_customer_waits_for_its_own_orders_only() -> Result<(), Box<dyn Error>> {
        time::pause();
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        // Two orders of each customer; the in-memory broker reads the key from a header.
        let orders: Vec<(Order, HeaderMap)> = [(1, "alice"), (2, "bob"), (3, "alice"), (4, "bob")]
            .into_iter()
            .map(|(id, customer)| {
                let mut headers = HeaderMap::new();
                headers.insert(PARTITION_KEY_HEADER, customer);
                (Order { id }, headers)
            })
            .collect();

        let started = Instant::now();
        for published in join_all(orders.iter().map(|(order, headers)| {
            broker
                .message(order)
                .with_headers(headers.clone())
                .to("orders")
                .publish()
        }))
        .await
        {
            published?;
        }

        // Each customer's two charges ran one after the other, beside the other customer's.
        assert_eq!(started.elapsed().as_secs(), 2);
        broker
            .subscriber("orders")
            .assert_called(4)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
