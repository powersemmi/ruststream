//! Delayed redelivery from the Subscribers guide: `retry_after` for the not-ready-yet case,
//! and per-element delays in a selective batch outcome.
//!
//! ```text
//! cargo run --example retry --features macros,memory,json -- run
//! ```

use std::time::Duration;

use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
// The derive and the pipeline's message type share the name in different namespaces: the derive
// is the macro `ruststream::Outgoing`, the value flowing through a publish transform is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, SlotContext};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:retry_after]
/// The not-ready-yet case: the upstream has not settled this payment, so an immediate
/// redelivery would just spin. Ask the broker to redeliver no sooner than five seconds from now.
#[subscriber("payments")]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if !payment.settled {
        return HandlerOutcome::retry_after(Duration::from_secs(5));
    }
    println!("payment {} settled", payment.id);
    HandlerOutcome::ack()
}
// --8<-- [end:retry_after]

// --8<-- [start:batch_retry_after]
/// Selective outcomes carry per-element delays: settled payments ack immediately, pending ones
/// come back in thirty seconds without holding up the rest of the batch.
#[subscriber("payments")]
async fn reconcile_batch(payments: &[Payment]) -> Vec<HandlerOutcome> {
    payments
        .iter()
        .map(|payment| {
            if payment.settled {
                HandlerOutcome::ack()
            } else {
                HandlerOutcome::retry_after(Duration::from_secs(30))
            }
        })
        .collect()
}
// --8<-- [end:batch_retry_after]

// --8<-- [start:mount]
/// A transform on the retry position: it stamps every copy with the slot it left through, so a
/// redelivery is recognisable downstream. The position is an `Out` slot, so its transforms read a
/// `SlotContext` like any other slot's.
struct DeferredStamp;

impl<Options> PublishTransform<ForSlot, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, cx: &SlotContext<'_>) {
        out.headers_mut()
            .insert("x-left-through", cx.slot().to_owned());
    }
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // --8<-- [start:declaration]
        // A poison message is one that never settles, and the cap is what ends it: after five
        // deliveries the payment goes to the dead-letter subject instead of coming back.
        b.include(reconcile)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead");
        // --8<-- [end:declaration]
        // Every registration already has a publisher for its copies, taken from the broker's
        // default policy. Naming one replaces it: another policy, another codec, a transform.
        // The position is an `Out` slot, so it takes the slot steps, and a copy carries the
        // delivery's own bytes, so the codec named here resolves the position and encodes
        // nothing.
        b.include(reconcile_batch.batch(nonzero!(64)))
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead")
            .out_retry(Publish)
            .codec(JsonCodec)
            .transform(DeferredStamp);
    })
}
// --8<-- [end:mount]
