use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A reply is a published message, so its type declares where it goes. Without the derive there is
// no declaration to read, and the clause's name has nothing to be the default of.
#[derive(Serialize)]
struct Receipt {
    id: u32,
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm);
    });
}
