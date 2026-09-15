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

use common::{Latch, MESSAGES, Order, Queue, Service};
use divan::Bencher;
use divan::counter::ItemsCount;
use futures::StreamExt;
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{IncomingMessage, Subscriber};
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

fn consuming() -> Service {
    common::service(MESSAGES, 0, |b| {
        b.include(consume);
    })
}

fn replying() -> Service {
    common::service(MESSAGES, 0, |b| {
        b.include(confirm);
    })
}

fn queue() -> Queue {
    common::queue(MESSAGES, 0)
}

fn decode_by_hand(message: &MemoryMessage) -> Order {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    black_box((order.id, order.quantity));
    order
}

#[divan::bench]
fn consume_json(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(consuming)
        .bench_local_values(|app| common::drain(&app));
}

#[divan::bench]
fn consume_json_hand(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(queue)
        .bench_local_values(|mut queue| {
            queue.runtime.block_on(async {
                let mut stream = std::pin::pin!(queue.subscriber.stream());
                for _ in 0..MESSAGES {
                    let message = stream
                        .next()
                        .await
                        .expect("a delivery")
                        .expect("a delivery");
                    black_box(decode_by_hand(&message));
                    message.ack().await.expect("the ack");
                }
            });
        });
}

#[divan::bench]
fn reply(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(replying)
        .bench_local_values(|app| common::drain(&app));
}

#[divan::bench]
fn reply_hand(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(queue)
        .bench_local_values(|mut queue| {
            queue.runtime.block_on(async {
                let mut stream = std::pin::pin!(queue.subscriber.stream());
                for _ in 0..MESSAGES {
                    let message = stream
                        .next()
                        .await
                        .expect("a delivery")
                        .expect("a delivery");
                    let order = decode_by_hand(&message);
                    let body = serde_json::to_vec(&Confirmation { id: order.id })
                        .expect("an encodable reply");
                    common::send_by_hand(&queue.publisher, "confirmations", &body, &[]).await;
                    message.ack().await.expect("the ack");
                }
            });
        });
}
