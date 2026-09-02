use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The reply's wire follows the reply type now, so the retired clause points at its
// replacement instead of reading as an unknown keyword.
#[subscriber("orders", publish_raw("orders-wire"))]
async fn handle(order: &Order) -> Vec<u8> {
    order.id.to_be_bytes().to_vec()
}

fn main() {}
