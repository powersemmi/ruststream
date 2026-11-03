//! A `with_broker` closure that drops the chain `include` opened has mounted nothing, so it does
//! not compile: the closure must return the chain, and `()` is not one. This is the shape every
//! scope had before the chain form, so it is also the message a service upgrading reads.

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream};
use ruststream::{OutSlot, Publisher, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(OutSlot)]
struct Audit;

#[subscriber("orders")]
async fn mirror(order: &Order, Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {
    let _app =
        RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(mirror).out(Audit, MemoryPublish);
        });
}
