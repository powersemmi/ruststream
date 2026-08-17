use ruststream::runtime::{HandlerResult, Out};
use ruststream::{Message, OutSlot, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Message, Serialize)]
struct Progress {
    percent: u8,
}

#[derive(Message, Serialize)]
struct Rogue {
    note: String,
}

#[derive(OutSlot)]
#[publishes(Progress = "orders.progress")]
struct Events;

// `Rogue` is in the parameter's declared message list but not in the slot's dictionary: there
// is no declared destination for it, and the definition does not compile.
#[subscriber("orders")]
async fn forward(
    _order: &Order,
    Out(out): Out<impl Publisher, Events, (Progress, Rogue)>,
) -> HandlerResult {
    let _ = out;
    HandlerResult::Ack
}

fn main() {}
