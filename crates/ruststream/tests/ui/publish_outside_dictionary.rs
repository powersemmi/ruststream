use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.progress")]
struct Progress {
    percent: u8,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.rogue")]
struct Rogue {
    note: String,
}

#[derive(OutSlot)]
#[publishes(Progress)]
struct Events;

// `Rogue` declares a destination of its own, but the slot does not publish it: the marker's
// dictionary is what the generated document reports as leaving the slot, so a publish outside
// it does not compile.
#[subscriber("orders")]
async fn forward(order: &Order, Out(out): Out<impl Publisher, Events>) -> HandlerOutcome {
    let _ = out
        .message(&Rogue {
            note: order.id.to_string(),
        })
        .publish()
        .await;
    HandlerOutcome::ack()
}

fn main() {}
