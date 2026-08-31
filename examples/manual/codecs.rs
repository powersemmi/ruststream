//! Codec selection without the `macros` feature: the ladder is the same, and every rung is a call
//! on the mount - the scope codec, the per-definition `codec(..)` override, and the
//! decode-failure policy next to it.
//!
//! ```text
//! cargo run --example manual_codecs --no-default-features --features memory,json,cbor
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The definition value `#[subscriber("orders")]` would have minted.
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("got order {}", order.id);
        ready(Ok(()))
    }
}

/// A second handler, to show a scope holding more than one subscription.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("audited order {}", order.id);
        ready(Ok(()))
    }
}

/// The handler behind the retrying registration below.
struct Strict;

impl Handle<Order> for Strict {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("strictly decoded order {}", order.id);
        ready(Ok(()))
    }
}

fn app() -> RustStream {
    let info = AppInfo::new("codecs", "0.1.0");
    // --8<-- [start:scope]
    // Every subscription in the scope decodes with CBOR: the scope codec is the default the
    // `include` family reads off the mount site, so no registration repeats it.
    RustStream::new(info)
        .with_broker_codec(MemoryBroker::new(), CborCodec, |b| {
            b.include(subscriber("orders", Receive).build());
            b.include(subscriber("audit", Audit).build());
        })
        // --8<-- [end:scope]
        .with_broker(MemoryBroker::new(), |b| {
            // --8<-- [start:per_handler]
            // one handler on a codec of its own: the override sits on the definition, so no
            // router is needed to keep the choice from reaching the neighbours
            b.include(subscriber("orders", Receive).codec(JsonCodec).build());
            // --8<-- [end:per_handler]

            // --8<-- [start:decode_failure]
            // A payload that fails to decode is redelivered instead of dropped. The policy is a
            // setting on the same chain, the counterpart of `on_failure(decode = retry)` on the
            // declaration.
            b.include(
                subscriber("orders", Strict)
                    .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Retry))
                    .build(),
            );
            // --8<-- [end:decode_failure]
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
