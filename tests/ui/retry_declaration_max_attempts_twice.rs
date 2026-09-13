use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::{nonzero, subscriber};
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

// A registration caps its deliveries once: the second `max_attempts(..)` has nothing to declare.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile)
            .max_attempts(nonzero!(3u32))
            .max_attempts(nonzero!(5u32));
    });
}
