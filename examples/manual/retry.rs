//! Delayed redelivery written without the `macros` feature: `retry_after` for the not-ready-yet
//! case, and per-element delays in a selective batch outcome.
//!
//! ```text
//! cargo run --example manual_retry --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
// The derive and the pipeline's message type share the name in different namespaces: the derive
// is the macro `ruststream::Outgoing`, the value flowing through a publish transform is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext};
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:retry_after]
/// The not-ready-yet case: the upstream has not settled this payment, so an immediate
/// redelivery would just spin. Ask the broker to redeliver no sooner than five seconds from now.
struct Reconcile;

impl Handle<Payment> for Reconcile {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !payment.settled {
            return ready(Err(HandlerOutcome::retry_after(Duration::from_secs(5))));
        }
        println!("payment {} settled", payment.id);
        ready(Ok(()))
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:batch_retry_after]
/// Selective outcomes carry per-element delays: settled payments ack immediately, pending ones
/// come back in thirty seconds without holding up the rest of the batch.
struct ReconcileBatch;

impl Handle<[Payment]> for ReconcileBatch {
    fn handle(
        &self,
        payments: &[Payment],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(Err(payments
            .iter()
            .map(|payment| {
                if payment.settled {
                    HandlerOutcome::ack()
                } else {
                    HandlerOutcome::retry_after(Duration::from_secs(30))
                }
            })
            .collect()))
    }
}
// --8<-- [end:batch_retry_after]

// --8<-- [start:mount]
/// A transform on the retry position: it stamps every copy with the subscription the delivery
/// came from, so a redelivery is recognisable downstream. Transforms here read the delivery being
/// retried, the way a reply's do.
struct DeferredStamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // --8<-- [start:declaration]
        // A poison message is one that never settles, and the cap is what ends it: after five
        // deliveries the payment goes to the dead-letter subject instead of coming back.
        b.include(subscriber("payments", Reconcile).build())
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead");
        // --8<-- [end:declaration]
        // --8<-- [start:named]
        // Where a subscription's descriptor addresses nothing - a wildcard, a filter, a pattern -
        // the mount site names where a copy goes, before the publisher.
        b.include(subscriber("payments.settled", Reconcile).build())
            .to("payments.retry")
            .max_attempts(nonzero!(5u32));
        // --8<-- [end:named]
        // Batches dispatch per batch rather than per delivery, and the batch input is what says
        // so; the batch size is the one parameter the mount owes the broker. Every registration
        // already has a publisher for its copies, taken from the broker's default policy, and
        // naming one replaces it: another policy, another codec, a transform.
        b.include(
            subscriber("payments", ReconcileBatch)
                .batch(nonzero!(64))
                .build(),
        )
        .max_attempts(nonzero!(5u32))
        .dead_letter("payments.dead")
        .out_retry(Publish)
        .codec(JsonCodec)
        .transform(DeferredStamp);
    })
}
// --8<-- [end:mount]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
