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
//! Consuming on the byte lane: the input type carries the payload itself, no codec is resolved
//! anywhere, and the handler reads the bytes. What is left in the difference is the dispatch.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;

/// The delivery read as bytes: no codec, the handler parses what it wants.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

#[subscriber("orders")]
async fn consume(frame: &Frame<'_>, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box(frame.0[0]);
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(consume);
    })
}

#[library_benchmark(config = common::config(0, 27))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_lane; benchmarks = service);
main!(library_benchmark_groups = consume_lane);
