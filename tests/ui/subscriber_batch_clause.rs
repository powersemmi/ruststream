use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The batch(..) clause is retired: the form is inferred from the payload type, and the error
// names that replacement.
#[subscriber(batch("orders"))]
async fn handle(orders: &[Order]) {
    let _ = orders.len();
}

fn main() {}
