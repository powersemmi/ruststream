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
//! Consuming a small JSON body on four dedicated threads keyed by partition, every delivery under
//! one key: the loop hands each delivery to the ring of the one thread the key maps to, and waits
//! on that ring alone when it is full. Both tools count the measured frame's thread alone, so the
//! numbers are what a delivery costs the app's runtime: the read, the handoff and the waits for
//! the key's ring. What the dedicated thread spends is its own.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, ONE_KEY, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;

#[subscriber("orders", threads(4, by_key))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending_with_headers(messages, ONE_KEY, |b| b.include(consume))
}

#[library_benchmark(config = common::config(0, 134))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_threads_by_key; benchmarks = service);
main!(library_benchmark_groups = consume_threads_by_key);
