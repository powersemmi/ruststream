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
//! Wall time per message over the in-process transport, framework against a hand-written loop.
//!
//! Informational, never a gate. It shows what an instruction count cannot - caches, branch
//! prediction, the scheduler - and it is noisy enough on a busy machine that a percent of
//! difference means nothing. The numbers that gate a change are the ones the other benchmarks
//! here produce.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Ready, Started};
use divan::Bencher;
use divan::counter::ItemsCount;
use ruststream::memory::prelude::*;
use serde::Serialize;

fn main() {
    divan::main();
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber("orders", publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

// The wall-clock harness times the closure it is handed, so the service is started and the queue
// filled here, outside the timing, rather than inside it as the instruction counts do it.
fn consuming() -> Ready {
    common::pending(MESSAGES, 0, |b| {
        b.include(consume);
    })
    .ready()
}

fn replying() -> Ready {
    common::pending(MESSAGES, 0, |b| {
        b.include(confirm);
    })
    .ready()
}

// The same consume on a runtime with worker threads, publishing inside the timing: what a
// delivery costs when the thread that produced it and the thread that handles it are different
// ones. The instruction counts cannot see this - an atomic read-modify-write is one instruction
// to callgrind whether the cache line is its own core's or another's.
fn consuming_across_threads() -> Started {
    common::pending_on(common::worker_runtime(), MESSAGES, 0, |b| {
        b.include(consume);
    })
    .started()
}

#[divan::bench]
fn consume_json(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(consuming)
        .bench_local_values(|ready| ready.runtime.block_on(ready.latch.drained()));
}

#[divan::bench]
fn reply(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(replying)
        .bench_local_values(|ready| ready.runtime.block_on(ready.latch.drained()));
}

#[divan::bench]
fn consume_json_across_threads(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(consuming_across_threads)
        .bench_local_values(|started| started.publish_and_drain());
}
