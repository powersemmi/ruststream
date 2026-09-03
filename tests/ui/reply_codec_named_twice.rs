use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, RustStream};
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

// The reply names its codec once: the second `.codec(..)` has no open slot to fill, so the step
// is gone from the type the first one produced.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm)
            .publisher(MemoryPublish)
            .codec(JsonCodec)
            .codec(CborCodec);
    });
}
