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
//! Reading a typed header contract off the delivery and publishing an event. The contract is
//! parsed before the body runs; the twin reads the same two entries and parses them itself.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Queue, Service};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{IncomingMessage, Subscriber};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The contract on the wire, which the fill below puts on every delivery.
const HEADERS: [(&str, &str); 2] = [("task_id", "17"), ("chunk_no", "3")];

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Meta {
    task_id: u64,
    chunk_no: u32,
}

#[derive(Debug, Serialize, Outgoing)]
struct Event {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Event)]
struct Audit;

#[subscriber("orders")]
async fn read_meta(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Headers(meta): Headers<Meta>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    ctx.state().arrived();
    black_box(meta.task_id);
    if out
        .message(&Event {
            id: black_box(order.id),
        })
        .to("events")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Service {
    common::service_with_headers(messages, &HEADERS, |b| {
        b.include(read_meta).out(Audit, Publish).build();
    })
}

/// The same read a typed contract does, written out: two header lookups and two parses.
fn meta_by_hand(message: &MemoryMessage) -> Meta {
    let headers = message.headers();
    let task_id = headers
        .get("task_id")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse().ok())
        .expect("the task id header");
    let chunk_no = headers
        .get("chunk_no")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse().ok())
        .expect("the chunk number header");
    Meta { task_id, chunk_no }
}

fn step(message: &MemoryMessage, latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Event {
        id: black_box(order.id),
    })
    .expect("an encodable event")
}

#[library_benchmark(config = common::config(5991))]
#[bench::read(app(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

#[library_benchmark(config = common::config(5001))]
#[bench::read(common::queue_with_headers(MESSAGES, &HEADERS))]
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
                black_box(meta_by_hand(&message).task_id);
                let body = step(&message, &latch);
                common::send_by_hand(&publisher, "events", &body, &[]).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

library_benchmark_group!(name = typed_headers_read; benchmarks = service, by_hand);
main!(library_benchmark_groups = typed_headers_read);
