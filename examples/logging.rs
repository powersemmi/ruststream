//! Colored console logging with zero boilerplate.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example logging \
//!     --features macros,memory,json,logging -- run
//! ```
//!
//! The `#[ruststream::app]` CLI installs the logger automatically on `run`, so the events RustStream
//! emits during dispatch (and the one below) show up with colored levels. Set `RUST_LOG` to tune
//! verbosity; without it the default `info` filter applies.

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    tracing::info!(order.id, "processing order");
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle, JsonCodec))
}
