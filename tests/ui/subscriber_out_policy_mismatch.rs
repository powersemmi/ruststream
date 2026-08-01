use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerResult, Out, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// Not what the attached MemoryPublish policy pairs into: the parameter cannot resolve.
struct ForeignPublisher;

#[subscriber("orders")]
async fn forward(order: &Order, Out(_out): Out<ForeignPublisher>) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(forward).publisher(MemoryPublish);
    });
}
