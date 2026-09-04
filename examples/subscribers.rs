//! The handler forms from the Subscribers guide: the basic contract, the context parameter, the
//! settings a mount site fills in, and the manual (macro-free) registration.
//!
//! ```text
//! cargo run --example subscribers --features macros,memory,json -- run
//! ```

use std::future::{Future, ready};

// The attribute and the value constructor share the name in different namespaces, so the one glob
// brings both into scope.
use ruststream::memory::prelude::*;
use serde::Deserialize;

// The manual registration at the bottom is documented by default, which is where the schema
// derive is owed; the attribute path asks for nothing.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// --8<-- [start:contract]
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:contract]

// --8<-- [start:context]
#[subscriber("orders")]
async fn with_context(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    if let Some(id) = ctx.headers().correlation_id() {
        println!("order {} correlates to {id}", order.id);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:context]

// --8<-- [start:deferred_name]
/// The by-name source with its value left out: the mount site names the subscription.
#[subscriber]
async fn audit(order: &Order) -> HandlerOutcome {
    println!("auditing order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:deferred_name]

// --8<-- [start:named_kind]
/// A named kind carrying only what it needs to exist; the value arrives at the mount site.
#[subscriber(MemorySource)]
async fn archive(order: &Order) -> HandlerOutcome {
    println!("archiving order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:named_kind]

// --8<-- [start:batch]
/// Settles a whole batch of orders in one go: the slice parameter is what says so.
#[subscriber("orders")]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    println!("settling {} orders", orders.len());
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

// --8<-- [start:raw_batch]
/// The raw element type: the derive gives the newtype the delivery's bytes, and with them both
/// input spellings, so a batch of frames is `&[Frame<'_>]`.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A batch of payloads: the batch shape without the decode step.
#[subscriber("frames")]
async fn ingest(frames: &[Frame<'_>]) -> HandlerOutcome {
    let bytes: usize = frames.iter().map(|frame| frame.0.len()).sum();
    println!("ingesting {} frames ({bytes} bytes)", frames.len());
    HandlerOutcome::ack()
}
// --8<-- [end:raw_batch]

// --8<-- [start:workers]
/// Up to 16 orders processed concurrently; global order is lost by design.
#[subscriber("orders", workers(16))]
async fn fan_out(order: &Order) -> HandlerOutcome {
    println!("processing order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:workers]

// --8<-- [start:workers_by_key]
/// 16 lanes keyed by the message's partition key: per-key order is preserved.
#[subscriber("orders", workers(16, by_key))]
async fn per_customer(order: &Order) -> HandlerOutcome {
    println!("processing order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:workers_by_key]

// --8<-- [start:batch_selective]
/// Retries only the entries that are not ready yet; the rest of the batch settles.
#[subscriber("orders")]
async fn reconcile(orders: &[Order]) -> Vec<HandlerOutcome> {
    orders
        .iter()
        .map(|order| {
            if order.id == 0 {
                HandlerOutcome::retry()
            } else {
                HandlerOutcome::ack()
            }
        })
        .collect()
}
// --8<-- [end:batch_selective]

/// How big a batch is, is the mount site's word, so both the name and the size land there.
#[subscriber]
async fn drain(orders: &[Order]) -> HandlerOutcome {
    println!("draining {} orders", orders.len());
    HandlerOutcome::ack()
}

/// A handler whose declarative settings are all left to the mount site.
#[subscriber]
async fn bill(order: &Order) -> HandlerOutcome {
    println!("billing order {}", order.id);
    HandlerOutcome::ack()
}

// The manual snippet below defines its handler type where the guide shows it, inside the builder
// closure, so the item follows the `include` statements above it on purpose.
#[allow(clippy::items_after_statements)]
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
        // The batch size is the one parameter a batch mount owes the broker: at most 64 orders per
        // call, whatever the broker builds its batches out of.
        b.include(settle.batch(nonzero!(64)));
        // --8<-- [end:batch_mount]
        b.include(ingest.batch(nonzero!(32)));
        b.include(reconcile.batch(nonzero!(64)));
        b.include(drain.name("orders").batch(nonzero!(128)));
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
        struct Inline;

        impl Handle<Order> for Inline {
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

        b.include(subscriber("orders", Inline).build());
        // --8<-- [end:manual]
    })
}
