use ruststream::runtime::{HandlerResult, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The attribute names the subscription, so the builder step that would name it does not apply.
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {
    let _renamed = handle.name("other");
}
