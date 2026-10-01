use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::{Outgoing, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The destination is declared; what is missing is the Serialize impl, so the reply cannot be
// encoded for publishing.
#[derive(Outgoing)]
struct Receipt {
    id: u32,
}

#[subscriber("orders", reply("receipts"))]
async fn handle(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
    });
}
