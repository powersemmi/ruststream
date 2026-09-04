use ruststream::nonzero;
use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A batch body: the mount site owes it a batch size, exactly once.
#[subscriber("orders")]
async fn handle(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

fn main() {
    // The size is the subscription's one batch parameter, so a second one has nothing left to
    // name.
    let _twice = handle.batch(nonzero!(128)).batch(nonzero!(64));
}
