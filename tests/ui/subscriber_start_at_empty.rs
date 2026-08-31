use ruststream::runtime::HandlerOutcome;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// An empty start_at() would mean "the broker's default", which is what no clause already does.
#[subscriber("orders", start_at())]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {}
