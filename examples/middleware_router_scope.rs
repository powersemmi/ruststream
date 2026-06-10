//! Router-scope middleware: a layer added with `Router::layer` wraps every handler registered
//! after it on that router.
//!
//! Handlers mounted directly on the broker scope are outside the router's stack (and the app has
//! none here). Planned for 0.3: routers inherit the application scope, and the two scopes
//! compose instead of being separate (see `middleware_app_scope.rs` for the other side).
//!
//! ```text
//! cargo run --example middleware_router_scope --features macros,memory,json -- run
//! ```

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, Context, Handler, HandlerResult, Identity, Layer, Router, RustStream, Stack,
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

impl<M: Send + Sync, H: Handler<M>> Handler<M> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        println!("router layer -> {}", ctx.name());
        self.0.handle(msg, ctx).await
    }
}

// --8<-- [start:router_scope]
fn routes() -> Router<MemoryBroker, Stack<LogLayer, Identity>> {
    // wraps every handler registered on this router after it
    let mut router = Router::new().layer(LogLayer);
    router.include(orders); //    wrapped by LogLayer
    router.include(shipments); // wrapped by LogLayer
    router
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("router-scope", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(routes());
        b.include(audit); // directly on the scope: outside the router's stack
    })
}
// --8<-- [end:router_scope]
