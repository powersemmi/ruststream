use ruststream::runtime::HandlerOutcome;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

fn stream(name: &'static str) -> ruststream::Name {
    ruststream::Name::new(name)
}

// The source type must be visible in the tokens: a free function hides it.
#[subscriber(stream("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {}
