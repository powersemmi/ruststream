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
//! The delivery side: what a message costs between the broker queue and the handler body.
//!
//! Each scenario runs twice - once through the service a user writes, once through a
//! hand-written loop over the same queue. See `common` for what is counted and why.

mod common;

use std::convert::Infallible;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Latch, MESSAGES};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::runtime::{BlanketLayer, Handler, Layer, RunningApp};
use ruststream::{BatchSubscriber, IncomingMessage, Subscriber};
use serde::Deserialize;
use tokio::runtime::Runtime;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// The same delivery read as bytes: no codec anywhere, the handler parses what it wants.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

// --- the service side -------------------------------------------------------------------

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber("orders")]
async fn consume_frame(frame: &Frame<'_>, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box(frame.0[0]);
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber("orders")]
async fn consume_batch(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

/// Asks the first `budget` deliveries to come back and acks everything after that, so the run
/// holds exactly one retry per published message and ends whichever order the copies arrive in.
///
/// Reading the retry count off the delivery would be the natural way to write this, and it does
/// not work here: the in-memory broker redelivers natively (`nack_after`) and counts nothing, so
/// neither the framework's retry header nor a `max_attempts` cap ever sees the copy.
#[subscriber("orders")]
async fn consume_retrying(order: &Order, ctx: &mut Context<'_, (), Retrying>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    let state = ctx.state();
    state.latch.arrived();
    if state.take_budget() {
        return HandlerOutcome::retry_after(Duration::ZERO);
    }
    HandlerOutcome::ack()
}

/// The retry scenario's application state: the latch every scenario has, plus the number of
/// deliveries still allowed to ask for a redelivery.
#[derive(Debug)]
struct Retrying {
    latch: Latch,
    budget: AtomicUsize,
}

impl Retrying {
    fn take_budget(&self) -> bool {
        self.budget
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }
}

/// A layer that adds a frame to the stack and nothing else: what the stack itself costs, with no
/// body of a user's own in the number.
#[derive(Clone)]
struct Passthrough;

struct Wrapped<H>(H);

impl<H> Layer<H> for Passthrough {
    type Handler = Wrapped<H>;

    fn layer(&self, inner: H) -> Wrapped<H> {
        Wrapped(inner)
    }
}

impl<M: Send + Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S> for Wrapped<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        self.0.handle(msg, ctx).await
    }
}

impl BlanketLayer for Passthrough {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Wrapped(handler)
    }
}

/// A started service with its queue already full.
struct Service {
    runtime: Runtime,
    latch: Latch,
    // Held so the service outlives the measured region; the handle is dropped with it.
    _app: RunningApp,
}

/// Arms the latch and fills the queue. The deliveries are in place before the measured region
/// opens, so a body pays for delivery and never for production.
fn filled(
    runtime: Runtime,
    broker: &MemoryBroker,
    latch: Latch,
    running: RunningApp,
    messages: usize,
    size: usize,
) -> Service {
    latch.expect(messages);
    common::fill(&broker.publisher(), &runtime, "orders", messages, size);
    Service {
        runtime,
        latch,
        _app: running,
    }
}

fn single(messages: usize, size: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            b.include(consume);
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    filled(runtime, &broker, latch, running, messages, size)
}

fn layered_one(messages: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .layer(Passthrough)
        .with_broker(broker.clone(), |b| {
            b.include(consume);
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    filled(runtime, &broker, latch, running, messages, 0)
}

fn layered_four(messages: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .layer(Passthrough)
        .layer(Passthrough)
        .layer(Passthrough)
        .layer(Passthrough)
        .with_broker(broker.clone(), |b| {
            b.include(consume);
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    filled(runtime, &broker, latch, running, messages, 0)
}

fn lane(messages: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            b.include(consume_frame);
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    filled(runtime, &broker, latch, running, messages, 0)
}

fn batched(messages: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            b.include(consume_batch.batch(nonzero!(64)));
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    filled(runtime, &broker, latch, running, messages, 0)
}

/// Every published message is handled twice: once as it arrives, once as the copy its retry
/// brought back, so the latch waits for twice the published count.
fn retrying(messages: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = Retrying {
        latch: latch.clone(),
        budget: AtomicUsize::new(messages),
    };
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            b.include(consume_retrying);
        });
    let running = runtime.block_on(app.start()).expect("the service starts");
    let service = filled(runtime, &broker, latch, running, messages, 0);
    service.latch.expect(messages * 2);
    service
}

// --- the hand-written side --------------------------------------------------------------

/// The same queue, filled the same way, with nothing between it and the loop below.
struct Queue {
    runtime: Runtime,
    subscriber: MemorySubscriber,
    messages: usize,
}

fn queue(messages: usize, size: usize) -> Queue {
    let runtime = common::runtime();
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe("orders");
    common::fill(&broker.publisher(), &runtime, "orders", messages, size);
    Queue {
        runtime,
        subscriber,
        messages,
    }
}

/// The hand-written per-message step: what the service's handler does, with the decode the
/// dispatcher would have done written out in front of it.
fn decode_by_hand(message: &MemoryMessage, latch: &Latch) {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    black_box((order.id, order.quantity));
    latch.arrived();
}

/// The same step for the byte lane: no codec, the body reads the payload itself.
fn read_by_hand(message: &MemoryMessage, latch: &Latch) {
    black_box(message.payload()[0]);
    latch.arrived();
}

// --- the measured bodies ----------------------------------------------------------------

#[library_benchmark(config = common::config(1))]
#[bench::small(single(MESSAGES, 0))]
#[bench::kilobyte(single(MESSAGES, 1024))]
fn consume_json(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(1))]
#[bench::small(queue(MESSAGES, 0))]
#[bench::kilobyte(queue(MESSAGES, 1024))]
fn consume_json_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        messages,
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
                decode_by_hand(&message, &latch);
                message.ack().await.expect("the ack");
            }
        });
    });
}

#[library_benchmark(config = common::config(1))]
#[bench::bytes(lane(MESSAGES))]
fn consume_lane(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(1))]
#[bench::bytes(queue(MESSAGES, 0))]
fn consume_lane_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        messages,
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
                read_by_hand(&message, &latch);
                message.ack().await.expect("the ack");
            }
        });
    });
}

#[library_benchmark(config = common::config(1))]
#[bench::one(layered_one(MESSAGES))]
#[bench::four(layered_four(MESSAGES))]
fn middleware(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(162))]
#[bench::of_64(batched(MESSAGES))]
fn consume_batch_64(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(97))]
#[bench::of_64(queue(MESSAGES, 0))]
fn consume_batch_64_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        messages,
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
                    decode_by_hand(&message, &latch);
                    message.ack().await.expect("the ack");
                }
            }
        });
    });
}

// The cold path: a delivery that asks to come back, and the copy that comes back. Reported,
// never gated - a retry is not on the steady-state path, and its cost is dominated by the
// republish the fallback makes.
#[library_benchmark(config = common::config_ungated())]
#[bench::once(retrying(MESSAGES))]
fn retry_copy(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

library_benchmark_group!(
    name = dispatch;
    benchmarks = consume_json, consume_json_hand, consume_lane, consume_lane_hand, middleware,
        consume_batch_64, consume_batch_64_hand, retry_copy
);
main!(library_benchmark_groups = dispatch);
