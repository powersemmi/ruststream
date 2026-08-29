//! Application-scope middleware without the `macros` feature: a layer added with
//! `RustStream::layer` wraps every handler registered after it, including handlers mounted through
//! `include_router` (the global stack composes around a router's own layers; see
//! `middleware_router_scope.rs` for that side).
//!
//! `layer` and `include_router` are plain runtime API, so the layer below is the same one the
//! macro version uses; what changes is that each handler is a named type registered with
//! `subscribe`.
//!
//! ```text
//! cargo run --example manual_middleware_app_scope --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    BlanketLayer, Handler, HandlerMetadata, Identity, Layer, Settle, Stack, typed,
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

impl<M: Send + Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> Settle {
        println!("app layer -> {}", ctx.name());
        self.0.handle(msg, ctx).await
    }
}

// Reaching router handlers requires the layer to be a BlanketLayer: the router hides its
// handlers' concrete types, so the wrap happens through this generic method at mount time.
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

// --8<-- [start:app_scope]
fn app() -> RustStream<Stack<LogLayer, Identity>> {
    RustStream::new(AppInfo::new("app-scope", "0.1.0"))
        // wraps every handler registered directly on a broker scope below
        .layer(LogLayer)
        .with_broker(MemoryBroker::new(), |b| {
            // wrapped by LogLayer
            b.subscribe(
                Name::new("orders"),
                typed(JsonCodec, Orders),
                HandlerMetadata::typed::<Order>("orders"),
            );
            // wrapped by LogLayer
            b.subscribe(
                Name::new("shipments"),
                typed(JsonCodec, Shipments),
                HandlerMetadata::typed::<Order>("shipments"),
            );

            // Mounted through a router: also wrapped by the app stack.
            b.include_router(Router::new().subscribe(
                Name::new("audit"),
                typed(JsonCodec, Audit),
                HandlerMetadata::typed::<Order>("audit"),
            ));
        })
}
// --8<-- [end:app_scope]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
