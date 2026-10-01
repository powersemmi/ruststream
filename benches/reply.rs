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
//! Replying: the handler returns a value, the runtime encodes it with the default codec and
//! publishes it where the reply type says. The in-memory broker keeps what it is handed, so the
//! reply costs the buffer the codec wrote.
//!
//! The same reply to a transport that reads the payload is `reply_lending`, a binary of its own:
//! a second mounting of the same handler in one binary gives every leaf of the dispatch stack a
//! second caller, LLVM keeps one out-of-line copy of each, and this scenario pays the frames -
//! about five percent per message, none of it the library's.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber("orders", reply)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(confirm);
    })
}

#[library_benchmark(config = common::config(2, 28))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply; benchmarks = service);
main!(library_benchmark_groups = reply);
