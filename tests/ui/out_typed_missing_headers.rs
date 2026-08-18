use ruststream::runtime::{HandlerResult, Out};
use ruststream::{Message, OutSlot, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, Deserialize)]
struct DoneMeta {
    task_id: u64,
}

#[derive(Message, Serialize)]
#[message(headers(DoneMeta))]
struct Done {
    key: String,
}

#[derive(OutSlot)]
#[publishes(Done = "orders.done")]
struct Events;

// `Done` declares a headers contract: publishing it without `.with_headers(&meta)` does not
// compile - the contract shape (`WithHeaders<DoneMeta>`) does not match the bare publish's
// `NoHeaders` requirement.
#[subscriber("orders")]
async fn forward(
    order: &Order,
    Out(out): Out<impl Publisher, Events, Done>,
) -> HandlerResult {
    let _ = out
        .publish_typed(&Done {
            key: order.id.to_string(),
        })
        .await;
    HandlerResult::Ack
}

fn main() {}
