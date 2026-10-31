// A handler that holds an `Rc` across an await mounts on the multi-thread runner: the error names
// the handler and says why, once, at the `include` that wrote it.
use std::rc::Rc;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[subscriber("orders")]
async fn on_order(order: &Order) -> HandlerOutcome {
    let held = Rc::new(order.id);
    tokio::task::yield_now().await;
    let _ = held;
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(on_order)
    });
}
