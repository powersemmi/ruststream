//! Router-scope middleware without the `macros` feature: a layer added with `Router::layer` wraps
//! every handler on that router when it is mounted.
//!
//! Handlers mounted directly on the broker scope are outside the router's stack (and the app has
//! none here). The app's global stack composes around the router's own layers (see
//! `middleware_app_scope.rs` for the other side).
//!
//! `Router` and its `layer` are plain runtime API, so the layer below is the same one the macro
//! version uses; a router groups hand-written registrations through the same `subscribe` method a
//! broker scope has.
//!
//! ```text
//! cargo run --example manual_middleware_router_scope --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    BlanketLayer, Handler, HandlerMetadata, Layer, RouterDef, Settle, typed,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// The definition value: `#[subscriber("orders")]` generates this struct and this impl.
struct Orders;

impl Handler<Order> for Orders {
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

struct Shipments;

impl Handler<Order> for Shipments {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got shipment for order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

struct Audit;

impl Handler<Order> for Audit {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("audited order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

#[derive(Clone)]
struct LogLayer;

struct Logged<H>(H);

impl<H> Layer<H> for LogLayer {
    type Handler = Logged<H>;
    fn layer(&self, inner: H) -> Logged<H> {
        Logged(inner)
    }
}

// A router hides its handlers' concrete types, so a router-scope layer must be a BlanketLayer.
impl BlanketLayer for LogLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Logged(handler)
    }
}

impl<M: Send + Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> Settle {
        println!("router layer -> {}", ctx.name());
        self.0.handle(msg, ctx).await
    }
}

// --8<-- [start:router_scope]
fn routes() -> impl RouterDef<MemoryBroker> {
    // wraps every handler on this router when it is mounted
    Router::new()
        .layer(LogLayer)
        // wrapped by LogLayer
        .subscribe(
            Name::new("orders"),
            typed(JsonCodec, Orders),
            HandlerMetadata::typed::<Order>("orders"),
        )
        // wrapped by LogLayer
        .subscribe(
            Name::new("shipments"),
            typed(JsonCodec, Shipments),
            HandlerMetadata::typed::<Order>("shipments"),
        )
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("router-scope", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(routes());
        // directly on the scope: outside the router's stack
        b.subscribe(
            Name::new("audit"),
            typed(JsonCodec, Audit),
            HandlerMetadata::typed::<Order>("audit"),
        );
    })
}
// --8<-- [end:router_scope]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
