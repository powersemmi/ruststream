//! The landing-page example: a one-handler service with no runtime boilerplate.
//!
//! ```text
//! cargo run --example quickstart --features macros,memory,json -- run
//! ```

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
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle, JsonCodec))
}
