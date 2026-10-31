//! The hooks a test registers on its assertion handles, against services written without the
//! `macros` feature. The test bodies are the ones of `examples/testing_hooks.rs`: the harness
//! runs the same hooks whichever way the service was declared.
//!
//! ```text
//! cargo test --example manual_testing_hooks --no-default-features --features testing,memory,json
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

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Confirmation {
    order_id: u64,
}

impl OutgoingDestination for Confirmation {
    type Form = FixedName;

    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

mod on_call {
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build())
        })
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
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tokio::sync::mpsc;

    use super::{Confirmation, Order};

    struct Confirm;

    impl Handle<Order, Confirmation> for Confirm {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
            ready(Ok(Confirmation { order_id: order.id }))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Confirm).reply().build())
                .out_reply(Publish)
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
