//! Batch handlers: a `&[T]` parameter runs the body once per batch, the mount site names the
//! batch size, and the body settles the batch as a whole or element by element. Each scenario is
//! a test.
//!
//! ```text
//! cargo test --example subscriber_batch --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

mod batch {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:batch]
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Order {
        id: u64,
    }

    /// One call per batch; the one outcome settles every order in it.
    #[subscriber("orders")]
    async fn store(orders: &[Order]) -> HandlerOutcome {
        tracing::info!(count = orders.len(), "stored in one round-trip");
        HandlerOutcome::ack()
    }

    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            // At most 64 orders per call.
            b.include(store.batch(nonzero!(64)))
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
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:per_element]
    const RECHECK: Duration = Duration::from_secs(30);

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "payments")]
    struct Payment {
        id: u64,
        cleared: bool,
    }

    /// One outcome per element, in slice order: only the uncleared payments come back.
    #[subscriber("payments")]
    async fn reconcile(payments: &[Payment]) -> Vec<HandlerOutcome> {
        payments
            .iter()
            .map(|payment| {
                if payment.cleared {
                    HandlerOutcome::ack()
                } else {
                    HandlerOutcome::retry_after(RECHECK)
                }
            })
            .collect()
    }
    // --8<-- [end:per_element]

    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("payments", "0.1.0"))
            .with_broker(broker, |b| b.include(reconcile.batch(nonzero!(64))))
    }

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
