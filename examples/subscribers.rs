//! The handler forms from the Subscribers guide: the basic contract, the context parameter, the
//! settings a mount site fills in, and the manual (macro-free) registration.
//!
//! ```text
//! cargo run --example subscribers --features macros,memory,json -- run
//! ```

use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemorySource};
// The attribute and the value constructor share the name in different namespaces, so the one glob
// brings both into scope.
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:contract]
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:contract]

// --8<-- [start:context]
#[subscriber("orders")]
async fn with_context(order: &Order, ctx: &mut Context<'_>) -> HandlerResult {
    if let Some(id) = ctx.headers().correlation_id() {
        println!("order {} correlates to {id}", order.id);
    }
    HandlerResult::Ack
}
// --8<-- [end:context]

// --8<-- [start:deferred_name]
/// The by-name source with its value left out: the mount site names the subscription.
#[subscriber]
async fn audit(order: &Order) -> HandlerResult {
    println!("auditing order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:deferred_name]

// --8<-- [start:named_kind]
/// A named kind carrying only what it needs to exist; the value arrives at the mount site.
#[subscriber(MemorySource)]
async fn archive(order: &Order) -> HandlerResult {
    println!("archiving order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:named_kind]

// --8<-- [start:batch]
/// Settles a whole page of orders in one go: the slice parameter is what says so.
#[subscriber("orders")]
async fn settle(orders: &[Order]) -> HandlerResult {
    println!("settling {} orders", orders.len());
    HandlerResult::Ack
}
// --8<-- [end:batch]

// --8<-- [start:raw_batch]
/// A batch of payloads: the batch shape without the decode step.
#[subscriber("frames")]
async fn ingest(frames: &[&[u8]]) -> HandlerResult {
    println!("ingesting {} frames", frames.len());
    HandlerResult::Ack
}
// --8<-- [end:raw_batch]

// --8<-- [start:workers]
/// Up to 16 orders processed concurrently; global order is lost by design.
#[subscriber("orders", workers(16))]
async fn fan_out(order: &Order) -> HandlerResult {
    println!("processing order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:workers]

// --8<-- [start:workers_by_key]
/// 16 lanes keyed by the message's partition key: per-key order is preserved.
#[subscriber("orders", workers(16, by_key))]
async fn per_customer(order: &Order) -> HandlerResult {
    println!("processing order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:workers_by_key]

// --8<-- [start:batch_selective]
/// Retries only the entries that are not ready yet; the rest of the page settles.
#[subscriber("orders")]
async fn reconcile(orders: &[Order]) -> Vec<HandlerResult> {
    orders
        .iter()
        .map(|order| {
            if order.id == 0 {
                HandlerResult::retry()
            } else {
                HandlerResult::Ack
            }
        })
        .collect()
}
// --8<-- [end:batch_selective]

/// Whether batches arrive at all is a property of the broker, so it is settled at the mount.
#[subscriber]
async fn drain(orders: &[Order]) -> HandlerResult {
    println!("draining {} orders", orders.len());
    HandlerResult::Ack
}

/// A handler whose declarative settings are all left to the mount site.
#[subscriber]
async fn bill(order: &Order) -> HandlerResult {
    println!("billing order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    let shard = 7;
    RustStream::new(AppInfo::new("subscribers", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
        b.include(with_context);
        // --8<-- [start:name_mount]
        b.include(audit.name(format!("audit-{shard}")));
        // --8<-- [end:name_mount]
        b.include(archive.name("archive"));
        // --8<-- [start:batch_mount]
        b.include(settle);
        // --8<-- [end:batch_mount]
        b.include(ingest);
        b.include(reconcile);
        // --8<-- [start:batch_buffered]
        // Client-side batching for subscriptions without native batches: close a batch at 128
        // deliveries, or 20 ms after its first one.
        b.include(
            drain
                .name("orders")
                .buffered(nonzero!(128), Duration::from_millis(20)),
        );
        // --8<-- [end:batch_buffered]
        // --8<-- [start:builder_settings]
        b.include(
            bill.name("orders")
                .workers(nonzero!(4))
                .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)),
        );
        // --8<-- [end:builder_settings]
        b.include(fan_out);
        b.include(per_customer);
        // --8<-- [start:manual]
        b.include(subscriber(
            "orders",
            |_order: &Order, _ctx: &mut Context| async { HandlerResult::Ack },
        ));
        // --8<-- [end:manual]
    })
}
