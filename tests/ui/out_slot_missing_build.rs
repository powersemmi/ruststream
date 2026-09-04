//! A slot chain dropped before `.build()` registers nothing, so the guard carrying the slots is
//! `#[must_use]` and the mistake is reported where the mount site is.
//!
//! Rust has no linear types, so the compiler cannot refuse the drop outright; the lint is what is
//! reachable. This file denies it, which is what the crates building with `-D warnings` do, so
//! the snapshot pins the wording of the diagnostic a service actually reads.
#![deny(unused_must_use)]

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
    let _app = RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(mirror).out(Audit, MemoryPublish);
        },
    );
}
