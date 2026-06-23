//! Application-scope middleware: a layer added with `RustStream::layer` wraps every handler
//! registered after it, including handlers mounted through `include_router` (the global stack
//! composes around a router's own layers; see `middleware_router_scope.rs` for that side).
//!
//! ```text
//! cargo run --example middleware_app_scope --features macros,memory,json -- run
//! ```

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, BlanketLayer, Context, Handler, HandlerResult, Identity, Layer, Router, RustStream,
    Settle, Stack,
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
#[ruststream::app]
fn app() -> RustStream<Stack<LogLayer, Identity>> {
    RustStream::new(AppInfo::new("app-scope", "0.1.0"))
        // wraps every handler registered directly on a broker scope below
        .layer(LogLayer)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(orders); //    wrapped by LogLayer
            b.include(shipments); // wrapped by LogLayer

            // Mounted through a router: also wrapped by the app stack.
            b.include_router(Router::new().include(audit));
        })
}
// --8<-- [end:app_scope]
