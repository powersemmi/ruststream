use ruststream::runtime::{HandlerResult, Out};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, Deserialize)]
struct DoneMeta {
    task_id: u64,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.done", headers = DoneMeta)]
struct Done {
    key: String,
}

#[derive(OutSlot)]
#[publishes(Done)]
struct Events;

// `Done` declares a headers contract: publishing it without `.with_headers(&meta)` does not
// compile - the empty headers position does not satisfy the declared `WithHeaders<DoneMeta>`.
#[subscriber("orders")]
async fn forward(order: &Order, Out(out): Out<impl Publisher, Events, Done>) -> HandlerResult {
    let _ = out
        .message(&Done {
            key: order.id.to_string(),
        })
        .publish()
        .await;
    HandlerResult::Ack
}

fn main() {}
