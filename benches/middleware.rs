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
//! Consuming through an application middleware stack. The layer forwards and does nothing else,
//! so what the pair shows is the price of the stack itself, at depth one and at depth four.

mod common;

use std::convert::Infallible;
use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Queue, Service};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{BlanketLayer, Handler, Layer};
use ruststream::{IncomingMessage, Subscriber};

#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
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

// The stack is part of the application type, so each depth builds its own app rather than taking
// the shared one.
fn one(messages: usize) -> Service {
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
    let app = runtime.block_on(app.start()).expect("the service starts");
    common::filled(
        runtime,
        latch,
        app,
        &broker.publisher(),
        messages,
        |publisher, runtime| {
            common::fill(publisher, runtime, common::INPUT, messages, 0);
        },
    )
}

fn four(messages: usize) -> Service {
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
    let app = runtime.block_on(app.start()).expect("the service starts");
    common::filled(
        runtime,
        latch,
        app,
        &broker.publisher(),
        messages,
        |publisher, runtime| {
            common::fill(publisher, runtime, common::INPUT, messages, 0);
        },
    )
}

fn step(message: &MemoryMessage, latch: &Latch) {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    black_box((order.id, order.quantity));
    latch.arrived();
}

#[library_benchmark(config = common::config(1))]
#[bench::one(one(MESSAGES))]
#[bench::four(four(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

// The twin of both depths: the same delivery with no stack at all.
#[library_benchmark(config = common::config(1))]
#[bench::plain(common::queue(MESSAGES, 0))]
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

library_benchmark_group!(name = middleware; benchmarks = service, by_hand);
main!(library_benchmark_groups = middleware);
