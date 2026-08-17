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
struct Done {
    key: String,
}

#[derive(OutSlot)]
#[publishes(Progress = "orders.progress", Done = "orders.done")]
struct Events;

// `Done` is in the slot's dictionary, but the parameter only declares `Progress`: the handler
// publishes what it declared, nothing else.
#[subscriber("orders")]
async fn forward(
    order: &Order,
    Out(out): Out<impl Publisher, Events, (Progress,)>,
) -> HandlerResult {
    let _ = out
        .publish_typed(&Done {
            key: order.id.to_string(),
        })
        .await;
    HandlerResult::Ack
}

fn main() {}
