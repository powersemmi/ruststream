use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// No Serialize impl: the reply cannot be encoded for publishing.
struct Receipt {
    id: u32,
}

#[subscriber("orders", publish("receipts"))]
async fn handle(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
    });
}
