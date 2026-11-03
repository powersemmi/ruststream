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
//! A post-settle continuation on every delivery: the handler acks and leaves work behind with
//! `and_after`, and the runner runs it once the ack is through. The scenario counts a delivery
//! when its continuation has run, so the measured region holds the hand-over to the runner and
//! the continuation's own task.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    let latch = ctx.state().clone();
    HandlerOutcome::ack().and_after(async move { latch.arrived() })
}

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| b.include(consume))
}

// Per delivery: the continuation's box and the task the runner spawns it as.
#[library_benchmark(config = common::config(2, 27))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = continuation; benchmarks = service);
main!(library_benchmark_groups = continuation);
