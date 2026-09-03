//! Logging every message with the app-global `TracingLayer`, written without the `macros`
//! feature: named handler types grouped in a `Router`, and a hand-written `main`.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example manual_logging_middleware \
//!     --no-default-features --features memory,json,logging
//! ```
//!
//! `TracingLayer` wraps every handler and emits a `tracing` event on each delivery (DEBUG on
//! arrival, INFO on ack, WARN on nack), so the handlers stay free of logging calls. Middleware is
//! runtime API, so none of it changes with the macros off; what does change is who installs the
//! console subscriber that renders the events - the generated CLI calls `logging::init` on `run`,
//! and here `main` calls it.
//!
//! The app-global `.layer(..)` reaches router handlers: `include_router` wraps each with the app's
//! stack, which must be a `BlanketLayer` (every bundled layer is). `TracingLayer` here applies to
//! both `Confirm` and `Reject`, mounted through the `routes` group.

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::runtime::layers::TracingLayer;
use ruststream::runtime::{Identity, Stack};
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    quantity: u32,
}

/// Accepts an order. The middleware logs the arrival and the resulting ack; no logging here.
struct Confirm;

impl Handle<Order> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

/// Rejects empty orders by requeueing. The middleware logs the nack at WARN with `requeue=true`.
struct Reject;

impl Handle<Order> for Reject {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if order.quantity == 0 {
            return ready(Err(HandlerOutcome::retry()));
        }
        ready(Ok(()))
    }
}

/// Builds the orders router. Broker-agnostic and middleware-agnostic: the app's global layer wraps
/// these handlers when the router is mounted.
// --8<-- [start:layered_router]
fn routes() -> impl RouterDef<MemoryBroker> {
    Router::new()
        .include(subscriber("orders", Confirm).build())
        .include(subscriber("returns", Reject).build())
}
// --8<-- [end:layered_router]

fn app() -> RustStream<Stack<TracingLayer, Identity>> {
    // The global layer is added before with_broker; include_router applies it to the router handlers.
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(TracingLayer::default())
        .with_broker(MemoryBroker::new(), |b| b.include_router(routes()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // What the generated CLI does on `run`: the layer only emits events, something has to render
    // them.
    ruststream::logging::init()?;
    app().run().await?;
    Ok(())
}
