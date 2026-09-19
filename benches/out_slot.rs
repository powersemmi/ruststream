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
//! Publishing from inside a handler through an `Out` slot, with one transform on the publish
//! path. The transform stamps a constant header, which is the cheapest step there is, so what the
//! pair shows is the position rather than the work inside it.
//!
//! Per-message publish settings are not in this number. They are the broker's own type
//! (`Publisher::Options`), and the in-memory broker declares `()`, so the core can only measure
//! the position where a setting would be resolved, never the resolving. A broker crate measures
//! that with its own options type.

mod common;

use std::hint::black_box;

use common::{Feed, Latch, MESSAGES, Order, Pending};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{ContextKind, Outgoing as OutgoingMessageView, PublishTransform, Reads};
use ruststream::{IncomingMessage, Subscriber};
use serde::Serialize;

/// An event the call site names a destination for.
#[derive(Debug, Serialize, Outgoing)]
struct Event {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Event)]
struct Audit;

/// One transform on the publish path: a constant header.
struct Stamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut OutgoingMessageView<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.headers_mut().insert("x-bench", b"1".to_vec());
    }
}

#[subscriber("orders")]
async fn audit(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    ctx.state().arrived();
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

fn app(messages: usize) -> Pending {
    common::pending(messages, 0, |b| {
        b.include(audit)
            .out(Audit, Publish)
            .transform(Stamp)
            .build();
    })
}

fn step(message: &MemoryMessage, latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Event {
        id: black_box(order.id),
    })
    .expect("an encodable event")
}

#[library_benchmark(config = common::config(20_000, 28))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

#[library_benchmark(config = common::config(16_000, 8))]
#[bench::first(common::feed(1, 0))]
#[bench::base(common::feed(MESSAGES, 0))]
#[bench::twice(common::feed(2 * MESSAGES, 0))]
fn by_hand(feed: Feed) {
    let mut subscriber = feed.subscribed();
    let publisher = feed.publisher();
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
                let body = step(&message, &latch);
                common::send_by_hand(&publisher, "events", &body, &[("x-bench", "1")]).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

library_benchmark_group!(name = out_slot; benchmarks = service, by_hand);
main!(library_benchmark_groups = out_slot);
