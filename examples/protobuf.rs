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
//! cargo run --example protobuf --no-default-features --features macros,memory -- run
//! ```

use ruststream::memory::prelude::*;

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
    println!("order {} of {}", order.id, order.sku);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
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
