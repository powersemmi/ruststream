use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{MessageInfo, OutSlot, Outgoing, Publisher, subscriber};
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

#[derive(MessageInfo, Serialize)]
struct Rogue {
    note: String,
}

#[derive(OutSlot)]
#[publishes(Progress)]
struct Events;

// `Rogue` says nothing about being sent - it derives the incoming-message metadata only - so it
// defines no message set, and naming it in the Out declaration does not compile.
#[subscriber("orders")]
async fn forward(
    _order: &Order,
    Out(out): Out<impl Publisher, Events, (Progress, Rogue)>,
) -> HandlerOutcome {
    let _ = out;
    HandlerOutcome::ack()
}

fn main() {}
