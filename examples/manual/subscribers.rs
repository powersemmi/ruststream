//! The handler forms from the Subscribers guide, written without the `macros` feature.
//!
//! Two levels appear here, and each section takes the one it needs. A handler is a named type with
//! an `impl Handler` (an `impl SliceHandler` for a batch, an `impl RawSliceHandler` for a raw one),
//! which `subscribe` / `subscribe_batch` mounts as it is. A *definition* adds the two impls
//! `include` reads - `Declared`, which is one `SubscriberBuilder::new(self, source)` plus the
//! settings chain, and `SubscriberDef` / `BatchDef` - and that is what carries the mount-site
//! settings builder (`.name`, `.workers`, `.on_failure`, `.buffered`).
//!
//! ```text
//! cargo run --example manual_subscribers --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::nonzero;
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, BatchDef, BatchResult, Declared, Decoded, FailurePolicies, FailurePolicy, Handler,
    HandlerMetadata, RawBytes, RawSliceHandler, RouterDef, Settle, SliceHandler, SubscriberBuilder,
    SubscriberDef, Workers, forms, typed,
};
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
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
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
/// Batches dispatch per page rather than per delivery, so the registration is `subscribe_batch` on
/// a router: a `BrokerScope` attaches single-delivery handlers only. The codec is named on the
/// chain, there being no declaration site for one to be read from.
fn batch_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .subscribe_batch(
            Name::new("orders"),
            SettlePage,
            HandlerMetadata::typed::<Order>("orders"),
        )
        .subscribe_batch(
            Name::new("orders"),
            Reconcile,
            HandlerMetadata::typed::<Order>("orders"),
        )
}
// --8<-- [end:batch_mount]

// --8<-- [start:raw_batch]
/// A batch of payloads: the batch shape without the decode step. `RawSliceHandler` borrows the
/// payloads straight out of the deliveries, so no codec takes part anywhere on this path.
///
/// This is the one form with no `subscribe` spelling - the raw batch adapter is reached through a
/// definition - so it is also the first place the two `include` impls appear. `Declared` is what
/// `include` dispatches on: its form token picks the mounting machinery and `declare` hands over
/// the settings builder; `BatchDef` is the definition proper (input kind, handler, source).
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

impl Declared for Ingest {
    type Form = forms::RawBatch;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("frames"))
    }
}

impl BatchDef for Ingest {
    type Input = RawBytes;
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("frames")
    }

    fn into_handler(self) -> Self {
        self
    }
}
// --8<-- [end:raw_batch]

// --8<-- [start:workers]
/// Up to 16 orders processed concurrently; global order is lost by design. Concurrency belongs to
/// the registration, so it is named where the handler is mounted: `Router::workers` applies to the
/// subscription just added.
struct FanOut;

impl Handler<Order> for FanOut {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("processing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn fan_out_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .subscribe(
            Name::new("orders"),
            typed(JsonCodec, FanOut),
            HandlerMetadata::typed::<Order>("orders"),
        )
        .workers(Workers::pool(nonzero!(16)))
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
        .subscribe(
            Name::new("orders"),
            typed(JsonCodec, PerCustomer),
            HandlerMetadata::typed::<Order>("orders"),
        )
        .workers(Workers::keyed(nonzero!(16)))
}
// --8<-- [end:workers_by_key]

// --8<-- [start:deferred_name]
/// The by-name source with its value left out: the mount site names the subscription.
///
/// `Unnamed<Name>` is no `SubscriptionSource` at all, so a mount that never calls `.name(..)` does
/// not compile. Naming it is what builds the source.
struct Audit;

impl Handler<Order> for Audit {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("auditing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

impl Declared for Audit {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Unnamed<Name>, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Unnamed::new())
    }
}

impl SubscriberDef for Audit {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Unnamed<Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn into_handler(self) -> Self {
        self
    }
}
// --8<-- [end:deferred_name]

// --8<-- [start:named_kind]
/// A named kind carrying only what it needs to exist; the value arrives at the mount site. The only
/// difference from the by-name form is which kind `Unnamed` stands in for, so `.name(..)` builds
/// the broker's own source instead of the generic one.
struct Archive;

impl Handler<Order> for Archive {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("archiving order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

impl Declared for Archive {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Unnamed<MemorySource>, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Unnamed::new())
    }
}

impl SubscriberDef for Archive {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Unnamed<MemorySource>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn into_handler(self) -> Self {
        self
    }
}
// --8<-- [end:named_kind]

/// Whether batches arrive at all is a property of the broker, so it is settled at the mount: this
/// batch definition leaves its source unnamed.
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

impl Declared for Drain {
    type Form = forms::Batch;
    type Settings = SubscriberBuilder<Self, Unnamed<Name>, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Unnamed::new())
    }
}

impl BatchDef for Drain {
    type Input = Decoded<Order>;
    type Handler = Self;
    type Source = Unnamed<Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn into_handler(self) -> Self {
        self
    }
}

/// A definition whose declarative settings are all left to the mount site: `declare` adds nothing
/// to the builder, so every step is still open there.
struct Bill;

impl Handler<Order> for Bill {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("billing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

impl Declared for Bill {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Unnamed<Name>, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Unnamed::new())
    }
}

impl SubscriberDef for Bill {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Unnamed<Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn into_handler(self) -> Self {
        self
    }
}

fn app() -> RustStream {
    let shard = 7;
    RustStream::new(AppInfo::new("subscribers", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // A plain handler registers directly: the source, the decoding and the metadata are the
        // three arguments `include` would have read off a definition.
        b.subscribe(
            Name::new("orders"),
            typed(JsonCodec, Handle),
            HandlerMetadata::typed::<Order>("orders"),
        );
        b.subscribe(
            Name::new("orders"),
            typed(JsonCodec, WithContext),
            HandlerMetadata::typed::<Order>("orders"),
        );
        // --8<-- [start:name_mount]
        b.include(Audit.name(format!("audit-{shard}")));
        // --8<-- [end:name_mount]
        b.include(Archive.name("archive"));
        b.include(Ingest);
        // --8<-- [start:batch_buffered]
        // Client-side batching for subscriptions without native batches: close a batch at 128
        // deliveries, or 20 ms after its first one.
        b.include(
            Drain
                .name("orders")
                .buffered(nonzero!(128), Duration::from_millis(20)),
        );
        // --8<-- [end:batch_buffered]
        // --8<-- [start:builder_settings]
        b.include(
            Bill.name("orders")
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
