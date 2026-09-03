//! A decoded subscriber input in a build with no codec feature: the mount has no codec to decode
//! the payload with, and nothing in the chain named one.
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;

#[derive(serde::Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    let _ = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle);
        });
}
