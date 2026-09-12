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
use ruststream::runtime::{Outgoing, SlotContext};
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
/// A transform on the retry position: it stamps every deferred copy with the slot it left
/// through, so a redelivery is recognisable downstream. The position is an `Out` slot, so its
/// transforms read a `SlotContext` like any other slot's.
struct DeferredStamp;

impl PublishTransform<ForSlot> for DeferredStamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, cx: &SlotContext<'_>) {
        out.headers_mut()
            .insert("x-left-through", cx.slot().to_owned());
    }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The publisher a deferred copy leaves through, named once per registration. The
        // in-memory broker honours the delay itself, so nothing here defers; a broker without
        // delayed redelivery of its own does, and then this position is what carries the delay.
        b.include(subscriber("payments", Reconcile).build())
            .out_retry(Publish);
        // Batches dispatch per batch rather than per delivery, and the batch input is what says
        // so; the batch size is the one parameter the mount owes the broker. The position is an
        // `Out` slot, so it takes the slot steps: the deferred copy carries the delivery's own
        // bytes, so the codec named here resolves the position and encodes nothing, while the
        // transforms run on the copy.
        b.include(
            subscriber("payments", ReconcileBatch)
                .batch(nonzero!(64))
                .build(),
        )
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
