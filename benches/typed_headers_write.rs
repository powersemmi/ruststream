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
//! Publishing with a typed header contract: the message type declares the contract, and the
//! publish does not compile without it. The twin builds the same two header entries by hand.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The header contract the message carries.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Meta {
    task_id: u64,
    chunk_no: u32,
}

/// The event with the contract attached: the publish does not compile without the headers.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "events", headers = Meta)]
struct Stamped {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Stamped)]
struct Stamps;

#[subscriber("orders")]
async fn stamp(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Out(out): Out<impl Publisher, Stamps>,
) -> HandlerOutcome {
    ctx.state().arrived();
    let meta = Meta {
        task_id: order.id,
        chunk_no: order.quantity,
    };
    if out
        .message(&Stamped {
            id: black_box(order.id),
        })
        .with_headers(&meta)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(stamp).out(Stamps, Publish).build();
    })
}

#[library_benchmark(config = common::config(10_000, 28))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = typed_headers_write; benchmarks = service);
main!(library_benchmark_groups = typed_headers_write);
