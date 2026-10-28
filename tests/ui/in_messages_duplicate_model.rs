use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

// A model appears once in a set: a second variant of the same model could never be picked.
#[derive(InMessages)]
enum OrderEvent {
    Created(OrderCreated),
    Placed(OrderCreated),
}

fn main() {}
