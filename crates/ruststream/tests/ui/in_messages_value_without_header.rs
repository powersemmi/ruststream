use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

// A value names the variant under a header the enum names; without the header there is
// nothing to read it from.
#[derive(InMessages)]
enum OrderEvent {
    #[in_messages(value = "created")]
    Created(OrderCreated),
}

fn main() {}
