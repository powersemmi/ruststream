use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, Reply, RustStream};
use ruststream::subscriber;
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
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

// A publish position is bound once. The reply's policy is named by the first `.out(Reply, ..)`,
// so the second has no unbound position left to bind.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm)
            .out(Reply, MemoryPublish)
            .out(Reply, MemoryPublish);
    });
}
