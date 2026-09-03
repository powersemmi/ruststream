//! Post-settle continuations from the Subscribers guide: `HandlerOutcome::ack().and_after(..)`
//! attaches a side effect that runs after the message is settled, without gating the ack decision
//! or affecting redelivery. The batch form attaches one continuation per element.
//!
//! ```text
//! cargo run --example post_settle --features macros,memory,json -- run
//! ```

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:single]
/// Ack the order, then fire a non-critical follow-up once it is acknowledged. The continuation is
/// at-most-once: if it is lost or panics, the already-acked order is not redelivered.
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    let id = order.id;
    HandlerOutcome::ack().and_after(async move {
        println!("order {id} acked; notifying downstream");
    })
}
// --8<-- [end:single]

// --8<-- [start:batch]
/// Per-element settlement: id 0 retries with no continuation, every other order acks and schedules
/// its own follow-up. The continuation rides with the element, so a batch settles each message and
/// its side effect independently.
#[subscriber("orders")]
async fn handle_page(orders: &[Order]) -> Vec<HandlerOutcome> {
    orders
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
        .collect()
}
// --8<-- [end:batch]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("post_settle", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
        b.include(handle_page);
    })
}
