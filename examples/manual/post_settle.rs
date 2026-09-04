//! Post-settle continuations written without the `macros` feature: `HandlerOutcome::ack()
//! .and_after(..)` attaches a side effect that runs after the message is settled, without gating
//! the ack decision or affecting redelivery. The batch form attaches one continuation per element.
//!
//! ```text
//! cargo run --example manual_post_settle --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// --8<-- [start:single]
/// Ack the order, then fire a non-critical follow-up once it is acknowledged. The continuation is
/// at-most-once: if it is lost or panics, the already-acked order is not redelivered.
///
/// `and_after` produces a `HandlerOutcome`, the verdict's `Err` side, so the outcome travels as
/// it is (a plain `Ok(())` would ack with no continuation).
struct Notify;

impl Handle<Order> for Notify {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let id = order.id;
        ready(Err(HandlerOutcome::ack().and_after(async move {
            println!("order {id} acked; notifying downstream");
        })))
    }
}
// --8<-- [end:single]

// --8<-- [start:batch]
/// Per-element settlement: id 0 retries with no continuation, every other order acks and schedules
/// its own follow-up. The continuation rides with the element, so a batch settles each message and
/// its side effect independently.
struct NotifyBatch;

impl Handle<[Order]> for NotifyBatch {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(Err(orders
            .iter()
            .map(|order| {
                if order.id == 0 {
                    HandlerOutcome::retry()
                } else {
                    let id = order.id;
                    HandlerOutcome::ack().and_after(async move {
                        println!("order {id} acked in batch; following up");
                    })
                }
            })
            .collect()))
    }
}
// --8<-- [end:batch]

fn app() -> RustStream {
    RustStream::new(AppInfo::new("post_settle", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Notify).build());
        // Batches dispatch per batch rather than per delivery, and the batch input is what says
        // so; the batch size is the one parameter the mount owes the broker.
        b.include(
            subscriber("orders", NotifyBatch)
                .batch(nonzero!(64))
                .build(),
        );
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
