//! The services of `examples/subscriber_batch.rs` written without the `macros` feature: a body
//! over `[T]` is a batch handler, and its error side is one outcome per element.
//!
//! ```text
//! cargo test --example manual_subscriber_batch --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

mod batch {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:batch]
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

    /// One call per batch; `Ok` settles every order in it.
    struct Store;

    impl Handle<[Order]> for Store {
        fn handle(
            &self,
            orders: &[Order],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            tracing::info!(count = orders.len(), "stored in one round-trip");
            ready(Ok(()))
        }
    }

    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            // At most 64 orders per call.
            b.include(subscriber("orders", Store).batch(nonzero!(64)).build())
        })
    }
    // --8<-- [end:batch]

    #[tokio::test]
    async fn three_orders_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        let tb = TestApp::start(app(broker)).await?;
        for id in [1, 2, 3] {
            publisher.message(&Order { id }).publish().await?;
        }
        tb.settle().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

mod per_element {
    use std::error::Error;
    use std::future::ready;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:per_element]
    const RECHECK: Duration = Duration::from_secs(30);

    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Payment {
        id: u64,
        cleared: bool,
    }

    // `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
    impl OutgoingDestination for Payment {
        type Form = FixedName;

        const DESTINATION: &'static str = "payments";
    }

    impl MessageHeaders for Payment {
        type Contract = NoHeaders;
    }

    /// One outcome per element, in slice order: only the uncleared payments come back.
    struct Reconcile;

    impl Handle<[Payment]> for Reconcile {
        fn handle(
            &self,
            payments: &[Payment],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            ready(Err(payments
                .iter()
                .map(|payment| {
                    if payment.cleared {
                        HandlerOutcome::ack()
                    } else {
                        HandlerOutcome::retry_after(RECHECK)
                    }
                })
                .collect()))
        }
    }

    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
            b.include(
                subscriber("payments", Reconcile)
                    .batch(nonzero!(64))
                    .build(),
            )
        })
    }
    // --8<-- [end:per_element]

    #[tokio::test(start_paused = true)]
    async fn only_the_uncleared_payment_comes_back() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        let tb = TestApp::start(app(broker)).await?;
        for (id, cleared) in [(1, true), (2, false), (3, true)] {
            publisher
                .message(&Payment { id, cleared })
                .publish()
                .await?;
        }
        tb.settle().await?;
        tb.advance(RECHECK).await?;

        let ids: Vec<Vec<u64>> = tb
            .broker::<MemoryBroker>()
            .subscriber("payments")
            .batches::<Payment>()
            .iter()
            .map(|batch| batch.iter().map(|payment| payment.id).collect())
            .collect();
        assert_eq!(ids, [vec![1, 2, 3], vec![2]]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
