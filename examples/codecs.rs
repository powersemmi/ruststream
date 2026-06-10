//! Codec selection from the Codecs guide: a per-scope codec, a per-handler override, and a
//! non-default decode-failure policy.
//!
//! ```text
//! cargo run --example codecs --features macros,memory,json,cbor -- run
//! ```

use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, Context, DecodeFailure, HandlerMetadata, HandlerResult, RustStream, SubscriberDef,
    typed,
};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[subscriber("audit")]
async fn audit(order: &Order) -> HandlerResult {
    println!("audited order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    let info = AppInfo::new("codecs", "0.1.0");
    // --8<-- [start:scope]
    RustStream::new(info)
        .with_broker_codec(MemoryBroker::new(), CborCodec, |b| {
            b.include(handle); // decodes with CborCodec
            b.include(audit); // also CborCodec
        })
        // --8<-- [end:scope]
        .with_broker(MemoryBroker::new(), |b| {
            // --8<-- [start:per_handler]
            // name the source and the codec for this one handler
            b.include_on(handle.source(), handle, JsonCodec);
            // --8<-- [end:per_handler]
            // --8<-- [start:decode_failure]
            let strict = typed(JsonCodec, |_order: &Order, _ctx: &mut Context| async {
                HandlerResult::Ack
            })
            .on_decode_failure(DecodeFailure::Requeue);
            b.handle(
                b.broker().subscribe("orders"),
                strict,
                HandlerMetadata::typed::<Order>("orders"),
            );
            // --8<-- [end:decode_failure]
        })
}
