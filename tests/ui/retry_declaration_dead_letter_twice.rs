use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
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

// A registration names one dead-letter destination: the second `dead_letter(..)` has nothing to
// declare.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile)
            .dead_letter("orders.dead")
            .dead_letter("orders.parked");
    });
}
