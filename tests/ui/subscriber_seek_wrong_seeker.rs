use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, Seek};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// Not the subscription's own seeker type: the runtime cannot mint it at startup.
struct ForeignSeeker;

#[subscriber(MemorySource::new("orders"))]
async fn handle(order: &Order, Seek(_seeker): Seek<ForeignSeeker>) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
    });
}
