use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, schemars::JsonSchema)]
struct Order {
    id: u32,
}

#[derive(Serialize, Outgoing, schemars::JsonSchema)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u32,
}

// The reply type fixes its destination and the clause names another one. The combination is
// legal: replies go to the type's destination, and startup reports the ignored name.
#[subscriber("orders", publish("audit"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

fn main() {
    let _app = RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm);
    });
}
