use ruststream::memory::{MemoryBroker, MemoryRequest};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream};
use ruststream::{OwnedTransactions, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The slot demands owned transactions, but the attached policy's live publisher (the memory
// requester) has no transactions at all: the include site fails to compile with the capability
// diagnostic.
#[subscriber("orders")]
async fn settle(order: &Order, Out(_tx): Out<impl OwnedTransactions>) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(settle).publisher(MemoryRequest);
    });
}
