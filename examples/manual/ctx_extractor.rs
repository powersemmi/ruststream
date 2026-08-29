//! The `Ctx<K>` extractor without the `macros` feature. `Ctx<K>` and `ContextField` are plain
//! public API, so the key is unchanged and the handler still binds the field through the same
//! `FromContext` resolution the attribute emits. What the handler writes out by hand is the context
//! type the attribute projected from the key: it is named in the `Handler` impl, and `subscriber`
//! reads it from there. Driven through the real dispatch path with the in-process `TestApp`
//! harness.
//!
//! ```text
//! cargo run --example manual_ctx_extractor --no-default-features --features testing,memory,json
//! ```

use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::prelude::*;
use ruststream::runtime::FromContext;
use ruststream::testing::TestApp;
use ruststream::{BuildContext, ContextField};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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
/// The handler body: `#[subscriber("orders")]` generates this struct and this impl.
struct Audit;

// The context type is written down rather than inferred: the attribute projected it from the key
// as `<PayloadLen as ContextField>::Context`, which is this broker context.
impl Handler<Order, DeliveryMeta> for Audit {
    async fn handle(&self, order: &Order, ctx: &mut Context<'_, DeliveryMeta>) -> Settle {
        // What the attribute emits for a `Ctx(len): Ctx<PayloadLen>` parameter: the field is
        // resolved off the delivery context before the body runs, and binds by the same pattern.
        let Ctx(len) =
            match <Ctx<PayloadLen> as FromContext<DeliveryMeta, ()>>::from_context(ctx).await {
                Ok(value) => value,
                Err(rejection) => return HandlerResult::from(rejection).into(),
            };

        println!("order {} arrived as {len} bytes", order.id);
        HandlerResult::Ack.into()
    }
}

// `subscriber` reads the broker context off the `Handler` impl, so naming `DeliveryMeta` there is
// the whole declaration: nothing repeats it at the mount, and `include` builds that context per
// delivery.
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Audit));
        });

    let tb = TestApp::start(app).await?;
    tb.publish("orders", &Order { id: 40 }).await?;

    println!("done");
    Ok(())
}
