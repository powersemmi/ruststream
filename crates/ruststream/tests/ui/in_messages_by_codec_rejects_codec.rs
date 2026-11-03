use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

#[derive(Deserialize, MessageInfo)]
struct OrderCancelled {
    id: u64,
}

#[derive(InMessages)]
#[in_messages(by_codec)]
enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

#[subscriber("orders")]
async fn on_event(_event: &OrderEvent) -> HandlerOutcome {
    HandlerOutcome::ack()
}

// The default codec is JSON, whose frame names no model: the set does not mount here.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(on_event))
}

fn main() {
    let _ = app();
}
