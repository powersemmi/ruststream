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
//! publishes it where the reply type says.
//!
//! Twice over, once per payload form. The in-memory broker keeps what it is handed, so its reply
//! costs the buffer the codec wrote; the lending sink beside it reads the payload and keeps
//! nothing, which is the form every transport that packs the body into a frame of its own
//! declares, and there the dispatch loop's own buffer carries every reply of the run.

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
        b.include(confirm);
    })
}

fn lending(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(confirm).out_reply(SinkPublish);
    })
}

#[library_benchmark(config = common::config(2, 28))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

// The same reply to a transport that reads the payload: the framework allocates nothing per
// message, so what is left in the steady state is the delivery the bus made.
#[library_benchmark(config = common::config(0, 30))]
#[bench::first(lending(1))]
#[bench::base(lending(MESSAGES))]
#[bench::twice(lending(2 * MESSAGES))]
fn service_lending(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply; benchmarks = service, service_lending);
main!(library_benchmark_groups = reply);
