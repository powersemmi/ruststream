//! The `TestApp` example from docs/guides/testing.md, kept compiling and passing here.
//! The guide embeds this file via snippet markers; keep the marked sections in sync with the
//! surrounding prose when editing.
#![cfg(all(
    feature = "testing",
    feature = "macros",
    feature = "memory",
    feature = "json"
))]

// --8<-- [start:handler]
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}
// --8<-- [end:handler]

// --8<-- [start:test]
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::testing::TestApp;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirms_valid_orders() {
    // The app under test: production wiring, in-memory broker.
    let app = RustStream::new(AppInfo::new("orders-test", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            let replies = TypedPublisher::new(b.broker().publisher());
            b.include_publishing(confirm, replies);
        },
    );

    // Start the harness (no connect, no server) and publish an order; the publish drives the
    // handler to a standstill before it returns.
    let tb = TestApp::start(app).await.expect("start harness");
    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 1, quantity: 2 })
        .await
        .expect("publish");

    // The handler decoded the order, acked, and published a confirmation.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1, quantity: 2 })
        .settled(HandlerResult::Ack);
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation {
            id: 1,
            accepted: true,
        });
}
// --8<-- [end:test]
