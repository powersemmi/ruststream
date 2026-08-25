//! Unit-test a service with the in-process [`TestApp`](ruststream::testing::TestApp) harness.
//!
//! The harness drives a built application with no network `connect` and no server: it injects input
//! through the broker bus, records what each handler received and how it settled, and exposes
//! per-broker assertions. `MemoryBroker` is a real broker here, not a test double - the harness
//! drives it (or any broker) through the same dispatch path the production runtime uses.
//!
//! ```text
//! cargo run --example testing --features testing,macros,memory,json
//! ```

use ruststream::memory::{MemoryBroker, MemoryPublisher};
use ruststream::runtime::{AppInfo, HandlerResult, PublishExt, RustStream};
use ruststream::testing::TestApp;
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
#[derive(Outgoing, Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Receipt {
    order_id: u64,
}

/// The application state: a publisher the handler uses to emit receipts.
struct AppState {
    receipts: MemoryPublisher,
}

/// Validates an order and publishes a receipt for it.
#[subscriber("orders")]
async fn handle_order(
    order: &Order,
    ctx: &mut ruststream::runtime::Context<'_, (), AppState>,
) -> HandlerResult {
    let receipt = Receipt { order_id: order.id };
    let payload = serde_json::to_vec(&receipt).expect("serialize");
    if ctx
        .state()
        .receipts
        .raw(&payload)
        .to("receipts")
        .publish()
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

/// Builds the service: one broker, the order handler, and a receipts publisher in state.
fn service() -> RustStream<ruststream::runtime::Identity, AppState> {
    let broker = MemoryBroker::new();
    let receipts = broker.publisher();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(
            move |()| async move { Ok::<_, std::convert::Infallible>(AppState { receipts }) },
        )
        .with_broker(broker, |b| b.include(handle_order))
}
// --8<-- [end:app]

// --8<-- [start:test]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(service()).await?;

    // Inject an order; the publish drives the handler to a standstill before returning.
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 42 })
        .to("orders")
        .publish()
        .await?;

    // The handler ran once, decoded the order, and acked.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 42 })
        .settled(HandlerResult::Ack);

    // It published the matching receipt downstream.
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { order_id: 42 });

    println!("ok: handler validated, acked, and published a receipt");
    Ok(())
}
// --8<-- [end:test]
