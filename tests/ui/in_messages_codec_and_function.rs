use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

fn by_kind(_message: &impl IncomingMessage) -> Option<OrderEventKind> {
    Some(OrderEventKind::Created)
}

struct ByRecord;

// A set picks its variant by one rule: a function or a codec-aware discriminator, not both.
#[derive(InMessages)]
#[in_messages(discriminate = by_kind, discriminate_with_codec = ByRecord)]
enum OrderEvent {
    Created(OrderCreated),
}

fn main() {}
