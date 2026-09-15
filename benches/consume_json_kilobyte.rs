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
//! The same delivery with a kilobyte of JSON: the struct is the same, so most of the body is
//! fields the decode skips. It shows what the framework costs when the payload, not the
//! framework, is the expensive part.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Queue, Service};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{IncomingMessage, Subscriber};

/// Bytes of JSON per delivery.
const BODY: usize = 1024;

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Service {
    common::service(messages, BODY, |b| {
        b.include(consume);
    })
}

fn step(message: &MemoryMessage, latch: &Latch) {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    black_box((order.id, order.quantity));
    latch.arrived();
}

#[library_benchmark(config = common::config(1))]
#[bench::kilobyte(app(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

#[library_benchmark(config = common::config(1))]
#[bench::kilobyte(common::queue(MESSAGES, BODY))]
fn by_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        messages,
        ..
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    common::measure(|| {
        runtime.block_on(async {
            let mut stream = std::pin::pin!(subscriber.stream());
            for _ in 0..messages {
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

library_benchmark_group!(name = consume_json_kilobyte; benchmarks = service, by_hand);
main!(library_benchmark_groups = consume_json_kilobyte);
