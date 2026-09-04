use ruststream::nonzero;
use ruststream::prelude::*;
use ruststream::runtime::SubscriberSettings;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// One message at a time, fanning out through a slot: the arena changes nothing about the cap,
// and there is still no batch to chunk.
#[subscriber("orders")]
async fn handle(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let _ = (order.id, out);
    HandlerOutcome::ack()
}

fn main() {
    let _capped = handle.batch(nonzero!(64));
}
