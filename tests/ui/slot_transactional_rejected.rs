use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream};
use ruststream::{OutSlot, Publisher, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(OutSlot)]
struct Audit;

#[subscriber("orders")]
async fn mirror(order: &Order, Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// `.transactional()` makes a page's replies one broker transaction, and a slot publish is one
// message with no page: a body opens its own slot transaction with `entry.begin()` instead.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(mirror)
            .out(Audit, MemoryPublish)
            .transactional()
            .build();
    });
}
