//! Router-scope middleware: a layer added with `Router::layer` wraps every handler on that
//! router when it is mounted.
//!
//! Handlers mounted directly on the broker scope are outside the router's stack (and the app has
//! none here). The app's global stack composes around the router's own layers (see
//! `middleware_app_scope.rs` for the other side).
//!
//! ```text
//! cargo run --example middleware_router_scope --features macros,memory,json -- run
//! ```

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, BlanketLayer, Context, Handler, HandlerResult, Layer, Router, RouterDef, RustStream,
    Settle,
};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn orders(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[subscriber("shipments")]
async fn shipments(order: &Order) -> HandlerResult {
    println!("got shipment for order {}", order.id);
    HandlerResult::Ack
}

#[subscriber("audit")]
async fn audit(order: &Order) -> HandlerResult {
    println!("audited order {}", order.id);
    HandlerResult::Ack
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
    fn apply<M, H>(&self, handler: H) -> impl Handler<M> + 'static
    where
        M: Send + Sync + 'static,
        H: Handler<M> + 'static,
    {
        Logged(handler)
    }
}

impl<M: Send + Sync, H: Handler<M>> Handler<M> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> Settle {
        println!("router layer -> {}", ctx.name());
        self.0.handle(msg, ctx).await
    }
}

// --8<-- [start:router_scope]
fn routes() -> impl RouterDef<MemoryBroker> {
    // wraps every handler on this router when it is mounted
    Router::new()
        .layer(LogLayer)
        .include(orders) //    wrapped by LogLayer
        .include(shipments) // wrapped by LogLayer
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("router-scope", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(routes());
        b.include(audit); // directly on the scope: outside the router's stack
    })
}
// --8<-- [end:router_scope]
