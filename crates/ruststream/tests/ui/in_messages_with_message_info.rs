use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

// A set is not a message: it declares several, each with its own name, so it carries no
// `MessageInfo` of its own.
#[derive(InMessages, MessageInfo)]
enum OrderEvent {
    Created(OrderCreated),
}

fn main() {}
