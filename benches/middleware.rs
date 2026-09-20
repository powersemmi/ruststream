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

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryBroker;
use ruststream::memory::prelude::*;
use ruststream::runtime::{BlanketLayer, Handler, Layer};

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
fn one(messages: usize) -> Pending {
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
    common::built(runtime, latch, broker, app, messages, 0)
}

fn four(messages: usize) -> Pending {
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
    common::built(runtime, latch, broker, app, messages, 0)
}

#[library_benchmark(config = common::config(0, 27))]
#[bench::first(one(1))]
#[bench::base(one(MESSAGES))]
#[bench::twice(one(2 * MESSAGES))]
fn service_one(app: Pending) {
    common::start_and_drain(app);
}

#[library_benchmark(config = common::config(0, 27))]
#[bench::first(four(1))]
#[bench::base(four(MESSAGES))]
#[bench::twice(four(2 * MESSAGES))]
fn service_four(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = middleware; benchmarks = service_one, service_four);
main!(library_benchmark_groups = middleware);
