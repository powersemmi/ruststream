//! A Core NATS service: one `#[subscriber]` handler bound to a subject, wired onto a [`NatsBroker`].
//!
//! `NatsBroker::new` is synchronous and does no I/O, so the whole service fits the
//! `#[ruststream::app]` macro just like the in-memory examples. The runtime connects the broker once
//! at startup (`Broker::connect`) before opening subscriptions, and the generated binary understands
//! `run` and `asyncapi gen`.
//!
//! Start a NATS server first (`nats-server`, or `docker run -p 4222:4222 nats`), then:
//!
//! ```text
//! cargo run --example nats_core --features macros,json -- run
//! ```
//!
//! Publish an order from another terminal with the NATS CLI:
//!
//! ```text
//! nats pub orders.created '{"id":1}'
//! ```

// --8<-- [start:handler]
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_nats::NatsBroker;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
