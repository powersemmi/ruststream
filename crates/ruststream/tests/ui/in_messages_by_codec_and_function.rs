use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

fn by_kind(_message: &impl IncomingMessage) -> Option<OrderEventKind> {
    Some(OrderEventKind::Created)
}

// A set picks its variant by one rule: a function or the codec's frame, not both.
#[derive(InMessages)]
#[in_messages(discriminate = by_kind, by_codec)]
enum OrderEvent {
    Created(OrderCreated),
}

fn main() {}
