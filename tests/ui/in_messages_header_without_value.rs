use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

#[derive(Deserialize, MessageInfo)]
struct OrderCancelled {
    id: u64,
}

// Under the header rule every variant declares the value that names it.
#[derive(InMessages)]
#[in_messages(header = "event")]
enum OrderEvent {
    #[in_messages(value = "created")]
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

fn main() {}
