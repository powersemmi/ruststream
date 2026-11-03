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
#[outgoing(name = "orders.done")]
struct Done {
    key: String,
}

#[derive(OutSlot)]
#[publishes(Progress, Done)]
struct Events;

// `Done` is in the slot's list, but the parameter only declares `Progress`: the handler
// publishes what it declared, nothing else.
#[subscriber("orders")]
async fn forward(
    order: &Order,
    Out(out): Out<impl Publisher, Events, (Progress,)>,
) -> HandlerOutcome {
    let _ = out
        .message(&Done {
            key: order.id.to_string(),
        })
        .publish()
        .await;
    HandlerOutcome::ack()
}

fn main() {}
