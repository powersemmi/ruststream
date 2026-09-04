use ruststream::nonzero;
use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// One message at a time: there is no batch for a cap to chunk.
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    let _capped = handle.batch(nonzero!(64));
}
