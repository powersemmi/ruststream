use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Retry, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[subscriber("orders")]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// A registration defers its retries through one publisher: the second `.out(Retry, ..)` has no
// open position to bind, because the first took it.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile)
            .out(Retry, MemoryPublish)
            .out(Retry, MemoryPublish);
    });
}
