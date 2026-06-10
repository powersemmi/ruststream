//! Application-scope middleware: a layer added with `RustStream::layer` wraps every handler
//! registered directly on a broker scope.
//!
//! It does NOT wrap handlers mounted through `include_router` - a router carries its own stack
//! (see `middleware_router_scope.rs`). Planned for 0.3: routers inherit the application scope,
//! and this distinction goes away.
//!
//! ```text
//! cargo run --example middleware_app_scope --features macros,memory,json -- run
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
        println!("app layer -> {}", ctx.name());
        self.0.handle(msg, ctx).await
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

            // Mounted through a router: NOT wrapped by the app stack (until 0.3).
            let mut router = Router::new();
            router.include(audit);
            b.include_router(router);
        })
}
// --8<-- [end:app_scope]
