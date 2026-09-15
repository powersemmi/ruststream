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

use common::{Latch, MESSAGES, Order, Queue, Service};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{IncomingMessage, Subscriber};
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

fn app(messages: usize) -> Service {
    common::service(messages, 0, |b| {
        b.include(stamp).out(Stamps, Publish).build();
    })
}

fn step(message: &MemoryMessage, latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Stamped {
        id: black_box(order.id),
    })
    .expect("an encodable event")
}

#[library_benchmark(config = common::config(15991))]
#[bench::write(app(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

#[library_benchmark(config = common::config(15001))]
#[bench::write(common::queue(MESSAGES, 0))]
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
                let headers = [("task_id", "17"), ("chunk_no", "3")];
                common::send_by_hand(&publisher, "events", &body, &headers).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

library_benchmark_group!(name = typed_headers_write; benchmarks = service, by_hand);
main!(library_benchmark_groups = typed_headers_write);
