use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The address is declared and the type carries no headers contract, so the publish is complete
// as written; what is missing is the `Serialize` derive that lets the codec encode it.
#[derive(Outgoing)]
#[outgoing(name = "orders.archived")]
struct Archived {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Archived)]
struct Events;

// Publishing a value the codec cannot encode does not compile, and the rejection carries serde's
// own guidance about the missing derive rather than reading as a missing method.
#[subscriber("orders")]
async fn forward(order: &Order, Out(out): Out<impl Publisher, Events, Archived>) -> HandlerOutcome {
    let _ = out.message(&Archived { id: order.id }).publish().await;
    HandlerOutcome::ack()
}

fn main() {}
