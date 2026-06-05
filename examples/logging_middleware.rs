//! Logging every message with the app-global `TracingLayer`, reaching handlers behind a `Router`.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example logging_middleware \
//!     --features macros,memory,json,logging -- run
//! ```
//!
//! `TracingLayer` wraps every handler and emits a `tracing` event on each delivery (DEBUG on
//! arrival, INFO on ack, WARN on nack), so the handlers stay free of logging calls. The `logging`
//! feature installs the console subscriber that renders those events; the generated
//! `#[ruststream::app]` CLI calls it on `run`.
//!
//! The app-global `.layer(..)` reaches router handlers: `include_router` wraps each with the app's
//! stack, which must be a `BlanketLayer` (every bundled layer is). `TracingLayer` here applies to
//! both `confirm` and `reject`, mounted through the `routes` module.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::layers::TracingLayer;
use ruststream::runtime::{AppInfo, HandlerResult, Identity, Router, RouterDef, RustStream, Stack};
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

/// Builds the orders router. Broker-agnostic and middleware-agnostic: the app's global layer wraps
/// these handlers when the router is mounted.
// --8<-- [start:layered_router]
fn routes() -> impl RouterDef<MemoryBroker> {
    Router::new().include(confirm).include(reject)
}
// --8<-- [end:layered_router]

#[ruststream::app]
fn app() -> RustStream<Stack<TracingLayer, Identity>> {
    // The global layer is added before with_broker; include_router applies it to the router handlers.
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(TracingLayer::default())
        .with_broker(MemoryBroker::new(), |b| b.include_router(routes()))
}
