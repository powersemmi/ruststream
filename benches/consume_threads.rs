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
//! Consuming a small JSON body on four dedicated threads: the loop on the app's runtime hands
//! each delivery to a thread's ring, and the thread decodes it, runs the handler and acks. Both
//! tools count the measured frame's thread alone, so the numbers are what a delivery costs the
//! app's runtime: the read and the handoff. What the dedicated threads spend is theirs, and
//! what the handoff costs in time is the wall-clock bench's to show.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;

#[subscriber("orders", threads(4))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(consume);
    })
}

#[library_benchmark(config = common::config(0, 130))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_threads; benchmarks = service);
main!(library_benchmark_groups = consume_threads);
