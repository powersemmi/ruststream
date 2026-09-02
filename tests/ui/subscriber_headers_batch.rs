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

// Headers are per-delivery; a batch spans many deliveries with as many header maps.
#[subscriber("orders")]
async fn bill(_orders: &[Order], Headers(_meta): Headers<Meta>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

fn main() {}
