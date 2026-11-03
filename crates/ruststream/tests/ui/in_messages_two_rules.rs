use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

fn by_kind(_message: &impl IncomingMessage) -> Option<OrderEventKind> {
    Some(OrderEventKind::Created)
}

// A set picks its variant by one rule: a function or a header, not both.
#[derive(InMessages)]
#[in_messages(discriminate = by_kind, header = "event")]
enum OrderEvent {
    #[in_messages(value = "created")]
    Created(OrderCreated),
}

fn main() {}
