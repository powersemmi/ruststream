use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A space of names: both placeholders have to be bound before the publish exists.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.{tenant}.{region}.v1")]
struct Placed {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Placed)]
struct Events;

// `region` is never bound, so the address builder still carries its unbound segment and has no
// publish terminal - the error names the segment that was forgotten.
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
