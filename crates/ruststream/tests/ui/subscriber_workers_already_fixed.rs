use ruststream::nonzero;
use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The attribute fixes the worker policy, so the mount site cannot disagree with it.
#[subscriber("orders", workers(2))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    let _widened = handle.workers(nonzero!(4));
}
