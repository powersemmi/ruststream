use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A bare attribute is the by-name source with its value left out, so the definition is not
// mountable until the mount site names it.
#[subscriber]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
    });
}
