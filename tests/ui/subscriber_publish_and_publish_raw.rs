use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// One reply, one destination: the encoded and the raw reply clause are mutually exclusive.
#[subscriber("orders", publish("a"), publish_raw("b"))]
async fn handle(order: &Order) -> Vec<u8> {
    order.id.to_be_bytes().to_vec()
}

fn main() {}
