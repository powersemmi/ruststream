use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

// The document lists every model by its `MessageInfo` name, so a model without the derive
// cannot be a variant.
#[derive(Deserialize)]
struct OrderCancelled {
    id: u64,
}

#[derive(InMessages)]
enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

fn main() {}
