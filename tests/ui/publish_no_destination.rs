use ruststream::runtime::{HandlerResult, Out};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The derive alone: this type is sent where the caller says, so the call site owes a `to(..)`.
#[derive(Outgoing, Serialize)]
struct Archived {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Archived)]
struct Events;

// Publishing without naming the destination does not compile: the builder's destination
// position is still `CallerName`, which is not a resolved one.
#[subscriber("orders")]
async fn forward(order: &Order, Out(out): Out<impl Publisher, Events, Archived>) -> HandlerResult {
    let _ = out.message(&Archived { id: order.id }).publish().await;
    HandlerResult::Ack
}

fn main() {}
