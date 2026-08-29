//! The handler forms from the Subscribers guide, written without the `macros` feature.
//!
//! A handler is a named type with an `impl Handler` (an `impl SliceHandler` for a batch, an
//! `impl RawSliceHandler` for a raw one). The value constructors - `subscriber`, `batch`,
//! `raw_batch` - bind it to its subscription source, and the result mounts with `include`,
//! chaining the declarative settings (`.name`, `.workers`, `.on_failure`, `.buffered`) the
//! attribute would otherwise fix.
//!
//! ```text
//! cargo run --example manual_subscribers --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:contract]
/// The handler contract without the attribute: a named type whose `impl Handler<Order>` carries
/// the body. The method returns `Settle`, so the outcome converts with `.into()`.
struct Handle;

impl Handler<Order> for Handle {
    // A body with nothing to await returns the future directly; a body that awaits writes
    // `async fn handle` instead, the shape the attribute generates.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:contract]

// --8<-- [start:context]
/// The context is a parameter of the trait method, so it is always in reach: nothing declares it.
struct WithContext;

impl Handler<Order> for WithContext {
    fn handle(&self, order: &Order, ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        if let Some(id) = ctx.headers().correlation_id() {
            println!("order {} correlates to {id}", order.id);
        }
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:context]

// --8<-- [start:batch]
/// Settles a whole page of orders in one go: `SliceHandler` is the batch counterpart of `Handler`,
/// and one `BatchResult::Uniform` settles every delivery behind the slice.
struct SettlePage;

impl SliceHandler<Order> for SettlePage {
    fn handle_slice(
        &self,
        orders: &[Order],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        println!("settling {} orders", orders.len());
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}
// --8<-- [end:batch]

// --8<-- [start:batch_selective]
/// Retries only the entries that are not ready yet; the rest of the page settles. One outcome per
/// element, in the order the slice was handed over.
struct Reconcile;

impl SliceHandler<Order> for Reconcile {
    fn handle_slice(
        &self,
        orders: &[Order],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::PerElement(
            orders
                .iter()
                .map(|order| {
                    if order.id == 0 {
                        HandlerResult::retry().into()
                    } else {
                        HandlerResult::ack().into()
                    }
                })
                .collect(),
        ))
    }
}
// --8<-- [end:batch_selective]

// --8<-- [start:batch_mount]
/// Batches dispatch per page rather than per delivery, so the constructor is `batch`: it demands
/// a batching subscriber of the source, exactly as a `batch(..)` attribute would.
fn batch_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(batch("orders", SettlePage))
        .include(batch("orders", Reconcile))
}
// --8<-- [end:batch_mount]

// --8<-- [start:raw_batch]
/// A batch of payloads: the batch shape without the decode step. `RawSliceHandler` borrows the
/// payloads straight out of the deliveries, so no codec takes part anywhere on this path; the
/// `raw_batch` constructor mounts it as-is.
struct Ingest;

impl RawSliceHandler for Ingest {
    fn handle_slice(
        &self,
        frames: &[&[u8]],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        println!("ingesting {} frames", frames.len());
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}
// --8<-- [end:raw_batch]

// --8<-- [start:workers]
/// Up to 16 orders processed concurrently; global order is lost by design. Concurrency belongs to
/// the registration, so it is chained where the handler is mounted.
struct FanOut;

impl Handler<Order> for FanOut {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("processing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn fan_out_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(subscriber("orders", FanOut).workers(nonzero!(16)))
}
// --8<-- [end:workers]

// --8<-- [start:workers_by_key]
/// 16 lanes keyed by the message's partition key: per-key order is preserved. The same slot as the
/// pool, filled with the keyed policy instead.
struct PerCustomer;

impl Handler<Order> for PerCustomer {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("processing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn per_customer_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(subscriber("orders", PerCustomer).workers_by_key(nonzero!(16)))
}
// --8<-- [end:workers_by_key]

// --8<-- [start:deferred_name]
/// A subscription named at the mount site: constructing over `Unnamed` leaves the source
/// unbuilt, and `Unnamed<Name>` is no `SubscriptionSource` at all, so a mount that never calls
/// `.name(..)` does not compile. Naming it is what builds the source.
struct Audit;

impl Handler<Order> for Audit {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("auditing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:deferred_name]

/// A named kind carrying only what it needs to exist; the value arrives at the mount site. The
/// only difference from the by-name form is which kind `Unnamed` stands in for, so `.name(..)`
/// builds the broker's own source instead of the generic one.
struct Archive;

impl Handler<Order> for Archive {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("archiving order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

/// Whether batches arrive at all is a property of the broker, so it is settled at the mount:
/// this handler's subscription buffers client-side.
struct Drain;

impl SliceHandler<Order> for Drain {
    fn handle_slice(
        &self,
        orders: &[Order],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        println!("draining {} orders", orders.len());
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

/// A handler whose declarative settings are all named at the mount site.
struct Bill;

impl Handler<Order> for Bill {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("billing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn app() -> RustStream {
    let shard = 7;
    RustStream::new(AppInfo::new("subscribers", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Handle));
        b.include(subscriber("orders", WithContext));
        // --8<-- [start:name_mount]
        b.include(subscriber(Unnamed::<Name>::new(), Audit).name(format!("audit-{shard}")));
        // --8<-- [end:name_mount]
        // --8<-- [start:named_kind]
        b.include(subscriber(Unnamed::<MemorySource>::new(), Archive).name("archive"));
        // --8<-- [end:named_kind]
        b.include(raw_batch("frames", Ingest));
        // --8<-- [start:batch_buffered]
        // Client-side batching for subscriptions without native batches: close a batch at 128
        // deliveries, or 20 ms after its first one.
        b.include(batch("orders", Drain).buffered(nonzero!(128), Duration::from_millis(20)));
        // --8<-- [end:batch_buffered]
        // --8<-- [start:builder_settings]
        b.include(
            subscriber("orders", Bill)
                .workers(nonzero!(4))
                .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)),
        );
        // --8<-- [end:builder_settings]
        b.include_router(fan_out_routes());
        b.include_router(per_customer_routes());
        b.include_router(batch_routes());
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
