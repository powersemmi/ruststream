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
//! Replying to a transport that reads the payload: the handler returns a value, the runtime
//! encodes it into the dispatch loop's own buffer and lends the bytes to a sink that keeps
//! nothing. This is the form every transport that packs the body into a frame of its own
//! declares, and the framework allocates nothing per message on it.
//!
//! A binary of its own rather than a second scenario in `reply`: mounted beside the in-memory
//! reply in one binary, it gives every leaf of that dispatch stack a second caller, and the
//! in-memory scenario loses its inlining to it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending, SinkPublish};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber("orders", publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(confirm).out_reply(SinkPublish);
    })
}

// The framework allocates nothing per message here, so what is left in the steady state is the
// delivery the bus made.
#[library_benchmark(config = common::config(0, 30))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_lending; benchmarks = service);
main!(library_benchmark_groups = reply_lending);
