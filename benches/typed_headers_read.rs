// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Reading a typed header contract off the delivery and publishing an event. The contract is
//! parsed before the body runs; the twin reads the same two entries and parses them itself.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The contract on the wire, which the fill below puts on every delivery.
const HEADERS: [(&str, &str); 2] = [("task_id", "17"), ("chunk_no", "3")];

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Meta {
    task_id: u64,
    chunk_no: u32,
}

#[derive(Debug, Serialize, Outgoing)]
struct Event {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Event)]
struct Audit;

#[subscriber("orders")]
async fn read_meta(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Headers(meta): Headers<Meta>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    ctx.state().arrived();
    black_box(meta.task_id);
    if out
        .message(&Event {
            id: black_box(order.id),
        })
        .to("events")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending_with_headers(messages, &HEADERS, |b| {
        b.include(read_meta).out(Audit, Publish).build();
    })
}

#[library_benchmark(config = common::config(2, 28))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = typed_headers_read; benchmarks = service);
main!(library_benchmark_groups = typed_headers_read);
