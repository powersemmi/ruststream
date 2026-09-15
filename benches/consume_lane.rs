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

use common::{Feed, Latch, MESSAGES, Pending};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{IncomingMessage, Subscriber};

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

fn step(message: &MemoryMessage, latch: &Latch) {
    black_box(message.payload()[0]);
    latch.arrived();
}

#[library_benchmark(config = common::config(0, 27))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

#[library_benchmark(config = common::config(0, 8))]
#[bench::first(common::feed(1, 0))]
#[bench::base(common::feed(MESSAGES, 0))]
#[bench::twice(common::feed(2 * MESSAGES, 0))]
fn by_hand(feed: Feed) {
    let mut subscriber = feed.subscribed();
    let latch = Latch::default();
    latch.expect(feed.messages);
    common::measure(|| {
        feed.runtime.block_on(async {
            let mut stream = std::pin::pin!(subscriber.stream());
            for _ in 0..feed.messages {
                let message = stream
                    .next()
                    .await
                    .expect("a delivery")
                    .expect("a delivery");
                step(&message, &latch);
                message.ack().await.expect("the ack");
            }
        });
    });
}

library_benchmark_group!(name = consume_lane; benchmarks = service, by_hand);
main!(library_benchmark_groups = consume_lane);
