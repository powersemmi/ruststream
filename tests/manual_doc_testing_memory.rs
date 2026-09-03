//! The macro-free counterpart of `tests/doc_testing_memory.rs`: the same reply handler and the
//! same `TestApp` test, with the definition written out instead of declared by `#[subscriber]`.
//!
//! What the harness reads is the definition, not the attribute, so the test half is unchanged.
#![cfg(all(feature = "testing", feature = "memory", feature = "json"))]

// --8<-- [start:handler]
use std::future::{Future, ready};

use ruststream::prelude::*;
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
struct Order {
    id: u64,
    quantity: u32,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the destination stays with the call site, which
// is what lets the test publish this type with `.to("orders")`.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

/// The reply body without the attribute: the reply type sits in the `Handle` impl's second
/// position, and the subscription source and the reply destination are named where the definition
/// is built.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}
// --8<-- [end:handler]

// --8<-- [start:test]
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::testing::TestApp;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirms_valid_orders() {
    // The app under test: production wiring, in-memory broker.
    let app = RustStream::new(AppInfo::new("orders-test", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            let replies = TypedPublisher::new(MemoryPublish);
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .publisher(replies)
                    .build(),
            );
        },
    );

    // Start the harness (no connect, no server) and publish an order; the publish drives the
    // handler to a standstill before it returns.
    let tb = TestApp::start(app).await.expect("start harness");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1, quantity: 2 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    // The handler decoded the order, acked, and published a confirmation.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1, quantity: 2 })
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation {
            id: 1,
            accepted: true,
        });
}
// --8<-- [end:test]
