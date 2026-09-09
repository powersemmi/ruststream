use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A name template is a space of destinations, and its placeholders are bound at the publish. The
// runtime publishes a reply on its own, with nothing to bind them from, so the type does not
// resolve on the reply path at all.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts.{tenant}")]
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
