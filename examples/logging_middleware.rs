//! Logging every message with the built-in `TracingLayer` middleware, on a `Router`.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example logging_middleware \
//!     --features macros,memory,json,logging -- run
//! ```
//!
//! `TracingLayer` is the ready-made middleware for this: it wraps every handler and emits a
//! `tracing` event on each delivery (DEBUG on arrival, INFO on ack, WARN on nack), so the handlers
//! stay free of logging calls. The `logging` feature installs the console subscriber that renders
//! those events; the generated `#[ruststream::app]` CLI calls it on `run`.
//!
//! The app's global `.layer(..)` does NOT reach handlers mounted through `include_router` - a
//! `Router` is built independently. So the middleware goes on the router itself with
//! `Router::layer(..)`, which wraps every handler registered after it. This mirrors how the
//! scaffolded projects (`ruststream new --broker nats`) group their handlers in a `routes` module.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::layers::TracingLayer;
use ruststream::runtime::{AppInfo, HandlerResult, Identity, Router, RustStream, Stack};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// Accepts an order. The middleware logs the arrival and the resulting ack; no logging here.
#[subscriber("orders")]
async fn confirm(order: &Order) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

/// Rejects empty orders by requeueing. The middleware logs the nack at WARN with `requeue=true`.
#[subscriber("returns")]
async fn reject(order: &Order) -> HandlerResult {
    if order.quantity == 0 {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

/// Builds the orders router with `TracingLayer` in front of every handler.
///
/// The layer is added first, so both `confirm` and `reject` registered after it are wrapped.
/// `TracingLayer::with_target("orders")` would route the events under a custom tracing target.
fn routes() -> Router<MemoryBroker, Stack<TracingLayer, Identity>> {
    let mut router = Router::new().layer(TracingLayer::default());
    router.include(confirm);
    router.include(reject);
    router
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(routes()))
}
