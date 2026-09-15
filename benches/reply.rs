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
//! publishes it where the reply type says. The twin encodes and publishes the same bytes itself.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Queue, Service};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{IncomingMessage, Subscriber};
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

fn app(messages: usize) -> Service {
    common::service(messages, 0, |b| {
        b.include(confirm);
    })
}

/// Decodes the delivery and encodes the answer, which is what the reply position does.
fn step(message: &MemoryMessage, latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Confirmation {
        id: black_box(order.id),
    })
    .expect("an encodable reply")
}

#[library_benchmark(config = common::config(5991))]
#[bench::json(app(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

#[library_benchmark(config = common::config(5001))]
#[bench::json(common::queue(MESSAGES, 0))]
fn by_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
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
                let body = step(&message, &latch);
                common::send_by_hand(&publisher, "confirmations", &body, &[]).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

library_benchmark_group!(name = reply; benchmarks = service, by_hand);
main!(library_benchmark_groups = reply);
