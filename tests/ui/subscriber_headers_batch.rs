use ruststream::runtime::{HandlerOutcome, Headers};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Deserialize)]
struct Meta {
    task_id: u64,
}

// Headers are per-delivery; on a batch each element pairs with its own contract through the
// `Message<H, T>` input, and the error names that replacement.
#[subscriber("orders")]
async fn bill(_orders: &[Order], Headers(_meta): Headers<Meta>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

fn main() {}
