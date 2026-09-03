use std::time::Duration;

use ruststream::nonzero;
use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A page body whose pages the framework's buffer already sizes.
#[subscriber("orders")]
async fn handle(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

fn main() {
    // The buffer already says how big a page is; a native page cap on top of it names the same
    // setting twice.
    let _twice = handle
        .buffered(nonzero!(128), Duration::from_millis(20))
        .batch(nonzero!(64));
}
