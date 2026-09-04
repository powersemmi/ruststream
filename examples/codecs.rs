//! Codec selection from the Codecs guide: a per-scope codec, a per-handler override, and a
//! non-default decode-failure policy.
//!
//! ```text
//! cargo run --example codecs --features macros,memory,json,cbor -- run
//! ```

use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[subscriber("audit")]
async fn audit(order: &Order) -> HandlerOutcome {
    println!("audited order {}", order.id);
    HandlerOutcome::ack()
}

// --8<-- [start:decode_failure]
/// A payload that fails to decode is redelivered instead of dropped.
#[subscriber("orders", on_failure(decode = retry))]
async fn strict(order: &Order) -> HandlerOutcome {
    println!("strictly decoded order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:decode_failure]

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
            // name the codec for this one handler by mounting it through a router
            b.include_router(Router::new().with_codec(JsonCodec).include(handle));
            // --8<-- [end:per_handler]
            b.include(strict);
        })
}
