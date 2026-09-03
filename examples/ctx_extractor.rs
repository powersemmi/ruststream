//! The `Ctx<K>` extractor: a broker context field arrives as a handler argument, like `State`
//! does for the application state. The key implements `ContextField`, which names the context
//! type it reads - so the handler needs no `&mut Context` parameter, and the `#[subscriber]`
//! macro projects the subscription's context type from the key. Driven through the real
//! dispatch path with the in-process `TestApp` harness.
//!
//! ```text
//! cargo run --example ctx_extractor --features testing,macros,memory,json
//! ```

use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{AppInfo, Ctx, HandlerOutcome, RustStream};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, ContextField, IncomingMessage, Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Outgoing, Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

// --8<-- [start:key]
// The broker's per-delivery context, built once per delivery; here it carries the payload
// size, standing in for an offset, a partition, or a delivery tag a real broker exposes.
struct DeliveryMeta {
    payload_len: usize,
}

impl BuildContext<MemoryMessage> for DeliveryMeta {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            payload_len: msg.payload().len(),
        }
    }
}

// The key: `ContextField` names the context it reads and yields an owned value, which is what
// lets it work as an extractor. Broker crates ship these next to their `Field` keys.
#[derive(Clone, Copy, Default)]
struct PayloadLen;

impl ContextField for PayloadLen {
    type Context = DeliveryMeta;
    type Value = usize;
    fn read(self, src: &DeliveryMeta) -> usize {
        src.payload_len
    }
}
// --8<-- [end:key]

// --8<-- [start:handler]
// The field arrives as an argument; no `&mut Context` parameter, no context type named - the
// macro projects it from the key.
#[subscriber("orders")]
async fn audit(order: &Order, Ctx(len): Ctx<PayloadLen>) -> HandlerOutcome {
    println!("order {} arrived as {len} bytes", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(audit));

    let tb = TestApp::start(app).await?;
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 40 })
        .to("orders")
        .publish()
        .await?;

    println!("done");
    Ok(())
}
