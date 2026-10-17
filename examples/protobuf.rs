//! A Protobuf service: the generated message rides the byte lanes, and no codec is resolved for
//! it anywhere.
//!
//! The type below is written out here, but it is what `prost-build` emits once its config carries
//! the two derives and the `#[wire(prost)]` line - a service generates it from `.proto` files
//! instead of typing it. Everything after that is an ordinary subscriber.
//!
//! Note the feature list: no codec is enabled, and none is needed.
//!
//! ```text
//! cargo test --example protobuf --no-default-features --features testing,macros,memory
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

// --8<-- [start:message]
#[derive(Clone, PartialEq, prost::Message, Outgoing, Serialized, Deserialized)]
#[outgoing(name = "orders")]
#[wire(prost)]
struct Order {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    sku: String,
}
// --8<-- [end:message]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.sku, "ordered");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
        // The outgoing half of the same declaration: the value encodes itself on the way out
        // too, so this publish names no codec either.
        b.after_startup(Publish, async move |publisher| {
            publisher
                .message(&Order {
                    id: 7,
                    sku: "widget".to_owned(),
                })
                .publish()
                .await
        });
    })
}
// --8<-- [end:app]

#[tokio::test]
async fn the_order_crosses_both_lanes() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    // The publish of `after_startup` reaches the handler.
    tb.settle().await?;
    let order = Order {
        id: 7,
        sku: "widget".to_owned(),
    };

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with_raw(&prost::Message::encode_to_vec(&order))
        .settled(HandlerOutcome::ack());
    Ok(())
}

// The scenario is the test above; the command in the header runs it.
fn main() {}
