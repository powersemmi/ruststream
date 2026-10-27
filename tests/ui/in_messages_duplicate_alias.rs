use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

type Placed = OrderCreated;

// A model appears once in a set, whatever it is spelled as: an alias names the same type.
#[derive(InMessages)]
enum OrderEvent {
    Created(OrderCreated),
    Placed(Placed),
}

fn main() {}
