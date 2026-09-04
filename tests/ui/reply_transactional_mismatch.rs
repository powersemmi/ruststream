use ruststream::memory::{MemoryBroker, MemoryRequest};
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

// `.transactional()` wraps a page's replies in one broker transaction, so the policy it marks has
// to pair into a `TransactionalPublisher`. The memory requester publishes and correlates replies
// but has no transactions, so the mount fails to compile with the capability diagnostic.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm.batch(nonzero!(8)))
            .out(Reply, MemoryRequest)
            .transactional();
    });
}
