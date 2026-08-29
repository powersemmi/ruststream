//! Codec selection without the `macros` feature: `typed(codec, handler)` carries the codec, so
//! every registration names one, and the decode-failure policy rides on the same wrapper.
//!
//! ```text
//! cargo run --example manual_codecs --no-default-features --features memory,json,cbor
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{FailurePolicy, Handler, HandlerMetadata, Settle, typed};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// The definition value `#[subscriber("orders")]` would have minted.
struct Handle;

impl Handler<Order> for Handle {
    // A body with nothing to await returns the future directly: `async fn` here would be an
    // unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

/// A second handler, to show a scope holding more than one subscription.
struct Audit;

impl Handler<Order> for Audit {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("audited order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

/// The handler behind the retrying registration below.
struct Strict;

impl Handler<Order> for Strict {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("strictly decoded order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn app() -> RustStream {
    let info = AppInfo::new("codecs", "0.1.0");
    RustStream::new(info)
        .with_broker(MemoryBroker::new(), |b| {
            // --8<-- [start:scope]
            // Every subscription in the scope decodes with CBOR, and says so itself. A scope codec
            // (`with_broker_codec`) is the default the `include` family reads off the mount site;
            // `subscribe` takes the handler already wrapped, so there is no mount site left to
            // reach for it - the codec travels in the `typed` call instead.
            b.subscribe(
                Name::new("orders"),
                typed(CborCodec, Handle),
                HandlerMetadata::typed::<Order>("orders"),
            );
            b.subscribe(
                Name::new("audit"),
                typed(CborCodec, Audit),
                HandlerMetadata::typed::<Order>("audit"),
            );
            // --8<-- [end:scope]
        })
        .with_broker(MemoryBroker::new(), |b| {
            // --8<-- [start:per_handler]
            // one handler on a codec of its own is the same call with a different codec in it: no
            // router is needed to keep the choice from reaching the neighbours
            b.subscribe(
                Name::new("orders"),
                typed(JsonCodec, Handle),
                HandlerMetadata::typed::<Order>("orders"),
            );
            // --8<-- [end:per_handler]

            // --8<-- [start:decode_failure]
            // A payload that fails to decode is redelivered instead of dropped. The policy sits on
            // the wrapper that owns the decode, which is the counterpart of `on_failure(decode =
            // retry)` on the declaration.
            b.subscribe(
                Name::new("orders"),
                typed(JsonCodec, Strict).on_decode_failure(FailurePolicy::Retry),
                HandlerMetadata::typed::<Order>("orders"),
            );
            // --8<-- [end:decode_failure]
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
