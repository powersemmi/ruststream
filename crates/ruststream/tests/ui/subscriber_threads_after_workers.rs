use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// Both clauses fill the one concurrency position.
#[subscriber("orders", workers(2), threads(2))]
async fn handle(order: &Order) {
    let _ = order.id;
}

fn main() {}
