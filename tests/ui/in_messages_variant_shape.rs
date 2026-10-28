use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

// Every variant of a set wraps exactly one model: a unit variant names no message.
#[derive(InMessages)]
enum OrderEvent {
    Created(OrderCreated),
    Closed,
}

fn main() {}
