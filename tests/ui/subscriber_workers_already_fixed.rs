use ruststream::nonzero;
use ruststream::runtime::{HandlerResult, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The attribute fixes the worker policy, so the mount site cannot disagree with it.
#[subscriber("orders", workers(2))]
async fn handle(order: &Order) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {
    let _widened = handle.workers(nonzero!(4));
}
