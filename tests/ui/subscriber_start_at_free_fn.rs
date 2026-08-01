use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

fn latest() -> ruststream::memory::MemoryPosition {
    ruststream::memory::MemoryPosition::end()
}

// The position type must be visible in the tokens: a free function hides it.
#[subscriber("orders", start_at(latest()))]
async fn handle(order: &Order) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {}
