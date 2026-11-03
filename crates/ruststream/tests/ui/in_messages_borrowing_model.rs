use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

/// A view over the payload bytes.
#[derive(Deserialized, MessageInfo)]
struct Frame<'a>(&'a [u8]);

// A variant owns its model past the delivery, so a view that borrows the payload cannot be
// one.
#[derive(InMessages)]
enum OrderEvent {
    Created(OrderCreated),
    Raw(Frame<'static>),
}

fn main() {}
