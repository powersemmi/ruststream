use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RequestReplyPublish, RustStream};
use ruststream::{OutSlot, Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.progress")]
struct Progress {
    percent: u8,
}

#[derive(OutSlot)]
#[publishes(Progress)]
struct Events;

// The first Out position stays the capability vocabulary, checked at the include site: the
// slot demands request / reply next to the declared messages, but the attached policy's live
// publisher (the plain memory publisher) does not correlate replies.
#[subscriber("orders")]
async fn forward(
    _order: &Order,
    Out(out): Out<impl RequestReplyPublish, Events, Progress>,
) -> HandlerOutcome {
    let _ = out;
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(forward).out(Events, MemoryPublish).build();
    });
}
