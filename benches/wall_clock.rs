// The harness macros generate the registration items and their paths; the crate's lints are
// written for the library surface, not for generated benchmark scaffolding.
#![allow(missing_docs, unused_qualifications, unreachable_pub)]
//! Wall time per message over the in-process transport, framework against a hand-written loop.
//!
//! Informational, never a gate. It shows what an instruction count cannot - caches, branch
//! prediction, the scheduler - and it is noisy enough on a busy machine that a percent of
//! difference means nothing. The numbers that gate a change are the ones in `dispatch` and
//! `publishing`.

mod common;

use std::convert::Infallible;
use std::hint::black_box;

use common::{Latch, MESSAGES};
use divan::Bencher;
use divan::counter::ItemsCount;
use futures::StreamExt;
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryBroker, MemoryMessage, MemoryPublisher, MemorySubscriber};
use ruststream::runtime::RunningApp;
use ruststream::{IncomingMessage, OutgoingMessage, Subscriber};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box(order.id);
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

struct Service {
    runtime: Runtime,
    latch: Latch,
    _app: RunningApp,
}

struct Queue {
    runtime: Runtime,
    subscriber: MemorySubscriber,
    publisher: MemoryPublisher,
}

fn service(replying: bool) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            if replying {
                b.include(confirm);
            } else {
                b.include(consume);
            }
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    latch.expect(MESSAGES);
    common::fill(&broker.publisher(), &runtime, "orders", MESSAGES, 0);
    Service {
        runtime,
        latch,
        _app: running,
    }
}

fn queue() -> Queue {
    let runtime = common::runtime();
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();
    common::fill(&publisher, &runtime, "orders", MESSAGES, 0);
    Queue {
        runtime,
        subscriber,
        publisher,
    }
}

fn decode_by_hand(message: &MemoryMessage) -> Order {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    black_box(order.quantity);
    order
}

#[divan::bench]
fn consume_json(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(MESSAGES))
        .with_inputs(|| service(false))
        .bench_local_values(|service| {
            service.runtime.block_on(service.latch.drained());
        });
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
        .with_inputs(|| service(true))
        .bench_local_values(|service| {
            service.runtime.block_on(service.latch.drained());
        });
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
                    queue
                        .publisher
                        .publish(OutgoingMessage::new("confirmations", &body), None)
                        .await
                        .expect("an in-process publish");
                    message.ack().await.expect("the ack");
                }
            });
        });
}
