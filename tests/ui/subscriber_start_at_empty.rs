use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// An empty start_at() would mean "the broker's default", which is what no clause already does.
#[subscriber("orders", start_at())]
async fn handle(order: &Order) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {}
