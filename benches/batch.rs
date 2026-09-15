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
//! Consuming in batches of 64: the subscription hands the handler a slice, and the runtime
//! settles every delivery in it. The twin reads the same batches through the broker's own batch
//! capability.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Queue, Service};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{BatchSubscriber, IncomingMessage};

#[subscriber("orders")]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Service {
    common::service(messages, 0, |b| {
        b.include(consume.batch(nonzero!(64)));
    })
}

fn step(message: &MemoryMessage, latch: &Latch) {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    black_box((order.id, order.quantity));
    latch.arrived();
}

#[library_benchmark(config = common::config(162))]
#[bench::of_64(app(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

#[library_benchmark(config = common::config(97))]
#[bench::of_64(common::queue(MESSAGES, 0))]
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
            let mut stream = std::pin::pin!(subscriber.batches(nonzero!(64)));
            let mut seen = 0;
            while seen < messages {
                let batch = stream.next().await.expect("a batch").expect("a batch");
                seen += batch.len();
                for message in batch {
                    step(&message, &latch);
                    message.ack().await.expect("the ack");
                }
            }
        });
    });
}

library_benchmark_group!(name = batch; benchmarks = service, by_hand);
main!(library_benchmark_groups = batch);
