use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A batch publishing handler replies with a `Vec`; a scalar return is rejected.
#[subscriber("orders", publish("done"))]
async fn handle(orders: &[Order]) -> u8 {
    orders.len() as u8
}

fn main() {}
