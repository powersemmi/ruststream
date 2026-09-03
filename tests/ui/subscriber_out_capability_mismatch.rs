use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RequestReplyPublish, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The slot demands request / reply, but the attached policy's live publisher (the plain memory
// publisher) does not correlate replies: the include site fails to compile with the capability
// diagnostic.
#[subscriber("orders")]
async fn forward(order: &Order, Out(_out): Out<impl RequestReplyPublish>) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(forward).publisher(MemoryPublish);
    });
}
