use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{OutSlot, Publisher, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(OutSlot)]
struct Audit;

// Two Out parameters binding the same slot marker: the attachments could not be told apart.
#[subscriber("orders")]
async fn forward(
    order: &Order,
    Out(_a): Out<impl Publisher, Audit>,
    Out(_b): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {}
