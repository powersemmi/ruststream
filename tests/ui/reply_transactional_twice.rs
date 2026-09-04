use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, Reply, RustStream, SubscriberSettings};
use ruststream::{nonzero, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize)]
struct Receipt {
    id: u32,
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

// A page's replies ride one transaction: the second `.transactional()` has no direct publish
// state left to mark, so the step reports the mark the first one already made.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm.batch(nonzero!(8)))
            .out(Reply, MemoryPublish)
            .transactional()
            .transactional();
    });
}
