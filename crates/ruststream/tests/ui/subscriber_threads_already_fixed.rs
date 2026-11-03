use ruststream::nonzero;
use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The attribute puts the subscription on dedicated threads; the mount site cannot add workers.
#[subscriber("orders", threads(2))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    let _pooled = handle.workers(nonzero!(4));
}
