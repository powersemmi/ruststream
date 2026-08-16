use ruststream::memory::MemoryPublisher;
use ruststream::runtime::{HandlerResult, Out};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A concrete publisher type couples the handler to one broker; the macro rejects it and names
// the impl-Trait form.
#[subscriber("orders")]
async fn forward(order: &Order, Out(_out): Out<MemoryPublisher>) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {}
