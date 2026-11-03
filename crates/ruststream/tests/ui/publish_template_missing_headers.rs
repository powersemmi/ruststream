use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, Deserialize)]
struct PlacedMeta {
    task_id: u64,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.{tenant}.v1", headers = PlacedMeta)]
struct Placed {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Placed)]
struct Events;

// The headers contract holds on the templated address too: every placeholder is bound here, so
// the address is complete, and what the publish still owes is the declared `with_headers(&meta)`.
// The generated terminal reports that contract, rather than reading as a missing method.
#[subscriber("orders")]
async fn forward(order: &Order, Out(out): Out<impl Publisher, Events, Placed>) -> HandlerOutcome {
    let _ = out
        .message(&Placed { id: order.id })
        .to()
        .tenant("acme")
        .publish()
        .await;
    HandlerOutcome::ack()
}

fn main() {}
