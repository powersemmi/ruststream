use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

// A set is an enum whose variants each wrap one model; a struct wraps one and is no set.
#[derive(InMessages)]
struct OrderEvent(OrderCreated);

fn main() {}
