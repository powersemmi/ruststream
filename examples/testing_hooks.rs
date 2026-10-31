//! The hooks a test registers on its assertion handles: code that runs at the moment a
//! subscription is called or a message is published, instead of reading the record once the
//! publish returned. Every scenario is a test of its own, the way a service writes it.
//!
//! ```text
//! cargo test --example testing_hooks --features testing,macros,memory,json
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

mod on_call {
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| b.include(accept))
    }

    // --8<-- [start:on-call]
    #[tokio::test]
    async fn every_order_is_counted_as_it_arrives() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .on_call(async move |orders| {
                let count = seen.fetch_add(1, Ordering::SeqCst) + 1;
                orders.assert_called(count).settled(HandlerOutcome::ack());
            });

        for id in 1..=3 {
            tb.broker::<MemoryBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        Ok(())
    }
    // --8<-- [end:on-call]
}

mod on_publish {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tokio::sync::mpsc;

    use super::{Confirmation, Order};

    #[subscriber("orders", reply)]
    async fn confirm(order: &Order) -> Confirmation {
        Confirmation { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(confirm).out_reply(Publish)
        })
    }

    // --8<-- [start:on-publish]
    #[tokio::test]
    async fn every_confirmation_reaches_the_audit() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let (audit, mut audited) = mpsc::channel(8);
        tb.broker::<MemoryBroker>()
            .published::<Confirmation>("confirmations")
            .on_publish(async move |confirmations| {
                let last = confirmations.decoded().pop();
                let _ = audit.send(last).await;
            });

        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        assert_eq!(
            audited.recv().await.flatten(),
            Some(Confirmation { order_id: 7 })
        );
        Ok(())
    }
    // --8<-- [end:on-publish]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
