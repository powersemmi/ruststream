//! The handler forms from the Subscribers guide, written without the `macros` feature.
//!
//! A handler is a named type with an `impl Handle`, and the input spelling picks the form: `&T`
//! for one decoded message, `&[T]` for a page, `&[F<'_>]` for a page of raw payloads, where `F`
//! is a type of the service's own that constructs itself from the bytes (`Deserialized`). The
//! one constructor - `subscriber` - binds the body to its subscription source, the declarative
//! settings (`.name`, `.workers`, `.on_failure`, `.buffered`) chain on the result, `.build()`
//! seals it, and `include` mounts it.
//!
//! ```text
//! cargo run --example manual_subscribers --no-default-features --features memory,json
//! ```

use std::convert::Infallible;
use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// --8<-- [start:contract]
/// The handler contract without the attribute: a named type whose `impl Handle<Order>` carries
/// the body. The axes it does not use - the reply, the injections, the broker context, the
/// application state - stay at their defaults, so the impl names only the input.
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
// --8<-- [end:contract]

// --8<-- [start:context]
/// The context is a parameter of the trait method, so it is always in reach: nothing declares it.
struct WithContext;

impl Handle<Order> for WithContext {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if let Some(id) = ctx.headers().correlation_id() {
            println!("order {} correlates to {id}", order.id);
        }
        ready(Ok(()))
    }
}
// --8<-- [end:context]

// --8<-- [start:batch]
/// Settles a whole page of orders in one go: the slice input is what says so, and a single
/// outcome settles every delivery behind the page.
struct SettlePage;

impl Handle<[Order]> for SettlePage {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        println!("settling {} orders", orders.len());
        ready(Ok(()))
    }
}
// --8<-- [end:batch]

// --8<-- [start:batch_selective]
/// Retries only the entries that are not ready yet; the rest of the page settles. One outcome per
/// element, in the order the slice was handed over.
struct Reconcile;

impl Handle<[Order]> for Reconcile {
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
                    HandlerOutcome::ack()
                }
            })
            .collect()))
    }
}
// --8<-- [end:batch_selective]

// --8<-- [start:batch_mount]
/// Batches dispatch per page rather than per delivery, and the page input is the whole
/// declaration: the mount demands a batching subscriber of the source, exactly as a `&[T]`
/// handler under `#[subscriber]` would. The page size is the one parameter a page mount owes the
/// broker: at most 64 orders per call, whatever the broker builds its pages out of.
fn batch_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(subscriber("orders", SettlePage).batch(nonzero!(64)).build())
        .include(subscriber("orders", Reconcile).batch(nonzero!(64)).build())
}
// --8<-- [end:batch_mount]

// --8<-- [start:raw_batch]
/// The raw element type. What `#[derive(Deserialized)]` would write is these two impls: the
/// construction, which borrows the bytes straight out of the delivery, and the `Input`
/// spelling that routes the type onto the self-deserializing lane - the page spelling
/// (`&[Frame<'_>]`) comes with it. No codec takes part anywhere on this path.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// A batch of payloads: the batch shape without the decode step.
struct Ingest;

impl<'p> Handle<[Frame<'p>]> for Ingest {
    fn handle(
        &self,
        frames: &[Frame<'p>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let bytes: usize = frames.iter().map(|frame| frame.0.len()).sum();
        println!("ingesting {} frames ({bytes} bytes)", frames.len());
        ready(Ok(()))
    }
}
// --8<-- [end:raw_batch]

// --8<-- [start:workers]
/// Up to 16 orders processed concurrently; global order is lost by design. Concurrency belongs to
/// the registration, so it is chained where the handler is mounted.
struct FanOut;

impl Handle<Order> for FanOut {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("processing order {}", order.id);
        ready(Ok(()))
    }
}

fn fan_out_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(subscriber("orders", FanOut).workers(nonzero!(16)).build())
}
// --8<-- [end:workers]

// --8<-- [start:workers_by_key]
/// 16 lanes keyed by the message's partition key: per-key order is preserved. The same slot as the
/// pool, filled with the keyed policy instead.
struct PerCustomer;

impl Handle<Order> for PerCustomer {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("processing order {}", order.id);
        ready(Ok(()))
    }
}

fn per_customer_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(
        subscriber("orders", PerCustomer)
            .workers_by_key(nonzero!(16))
            .build(),
    )
}
// --8<-- [end:workers_by_key]

// --8<-- [start:deferred_name]
/// A subscription named at the mount site: constructing over `Unnamed` leaves the source
/// unbuilt, and `Unnamed<Name>` is no `SubscriptionSource` at all, so a mount that never calls
/// `.name(..)` does not compile. Naming it is what builds the source.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("auditing order {}", order.id);
        ready(Ok(()))
    }
}
// --8<-- [end:deferred_name]

/// A named kind carrying only what it needs to exist; the value arrives at the mount site. The
/// only difference from the by-name form is which kind `Unnamed` stands in for, so `.name(..)`
/// builds the broker's own source instead of the generic one.
struct Archive;

impl Handle<Order> for Archive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("archiving order {}", order.id);
        ready(Ok(()))
    }
}

/// How big a page is, is the mount site's word, so the size lands there with the name.
struct Drain;

impl Handle<[Order]> for Drain {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        println!("draining {} orders", orders.len());
        ready(Ok(()))
    }
}

/// A handler whose declarative settings are all named at the mount site.
struct Bill;

impl Handle<Order> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("billing order {}", order.id);
        ready(Ok(()))
    }
}

fn app() -> RustStream {
    let shard = 7;
    RustStream::new(AppInfo::new("subscribers", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Receive).build());
        b.include(subscriber("orders", WithContext).build());
        // --8<-- [start:name_mount]
        b.include(
            subscriber(Unnamed::<Name>::new(), Audit)
                .name(format!("audit-{shard}"))
                .build(),
        );
        // --8<-- [end:name_mount]
        // --8<-- [start:named_kind]
        b.include(
            subscriber(Unnamed::<MemorySource>::new(), Archive)
                .name("archive")
                .build(),
        );
        // --8<-- [end:named_kind]
        b.include(subscriber("frames", Ingest).batch(nonzero!(32)).build());
        b.include(subscriber("orders", Drain).batch(nonzero!(128)).build());
        // --8<-- [start:builder_settings]
        b.include(
            subscriber("orders", Bill)
                .workers(nonzero!(4))
                .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip))
                .build(),
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
