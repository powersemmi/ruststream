use ruststream::runtime::{FromHeaders, HandlerResult};
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
#[subscriber(batch("orders"))]
async fn bill(_orders: &[Order], FromHeaders(_meta): FromHeaders<Meta>) -> HandlerResult {
    HandlerResult::Ack
}

fn main() {}
