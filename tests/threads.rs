//! `threads(n)` under the test harness, and the main-runtime key beside a broker's context key.
//!
//! `TestApp` runs a `threads(n)` subscription as `n` workers on the test's own runtime, so these
//! tests assert what the harness keeps of the declaration: the concurrency, the per-key order and
//! the clock. Placement itself is the subject of `threads_placement.rs`.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::Order;
use futures::future::join_all;
use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream::{BuildContext, ContextField, HeaderMap};
use tokio::sync::Barrier;

/// A pool that ran its deliveries one at a time parks on the barrier forever; the deadline turns
/// that into a failed assertion.
const CONCURRENCY_DEADLINE: Duration = Duration::from_secs(5);

/// A broker-style context with one field, so a handler can take a broker key beside the
/// main-runtime one.
struct Meta {
    len: usize,
}

impl BuildContext<MemoryMessage> for Meta {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            len: msg.payload().len(),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct PayloadLen;

impl ContextField for PayloadLen {
    type Context = Meta;
    type Value = usize;
    fn read(self, src: &Meta) -> usize {
        src.len
    }
}

/// The main-runtime key first: the subscription's context still comes from the broker key.
#[subscriber("mixed")]
async fn mixed(
    _order: &Order,
    Ctx(_main): Ctx<MainRuntime>,
    Ctx(len): Ctx<PayloadLen>,
) -> HandlerOutcome {
    if len > 0 {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_main_runtime_key_sits_beside_a_broker_key() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(mixed);
        });
    let tb = TestApp::start(app).await.expect("startup");
    tb.message(&Order { id: 1 })
        .to("mixed")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("mixed")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
}

/// Passes the barrier only with four deliveries in flight at once.
#[subscriber("crunch", threads(4))]
async fn crunch(_job: &Order, ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    HandlerOutcome::ack()
}

/// Under the harness the threads run as that many workers: all four deliveries are in flight at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_threads_run_as_that_many_workers() {
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Barrier::new(4))))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(crunch);
        });
    let tb = TestApp::start(app).await.expect("startup");
    let jobs: Vec<Order> = (1..=4).map(|id| Order { id }).collect();
    let published = tokio::time::timeout(
        CONCURRENCY_DEADLINE,
        join_all(
            jobs.iter()
                .map(|job| tb.message(job).to("crunch").publish()),
        ),
    )
    .await
    .expect("the subscription must hold its deliveries in flight at once");
    for result in published {
        result.expect("publish");
    }
    tb.broker::<MemoryBroker>()
        .subscriber("crunch")
        .assert_called(4)
        .settled(HandlerOutcome::ack());
}

/// Records nothing of its own: the id says which key a delivery carries, so per-key order reads
/// off the recorded deliveries.
#[subscriber("keyed", threads(4, by_key))]
async fn keyed(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    tokio::task::yield_now().await;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_threads_keep_each_key_in_order() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(keyed);
        });
    let tb = TestApp::start(app).await.expect("startup");
    keys_stay_in_order(&tb, "keyed").await;
}

/// Publishes two interleaved keys to `subject` and asserts each key's deliveries keep their order.
async fn keys_stay_in_order(tb: &TestApp<()>, subject: &'static str) {
    const PER_KEY: u32 = 10;
    const BETA_BAND: u32 = 100;
    let keyed_input = |key: &'static str, id: u32| {
        let mut headers = HeaderMap::new();
        headers.insert("partition-key", key);
        (Order { id }, headers)
    };
    let inputs: Vec<_> = (1..=PER_KEY)
        .flat_map(|id| {
            [
                keyed_input("alpha", id),
                keyed_input("beta", id + BETA_BAND),
            ]
        })
        .collect();
    for result in join_all(inputs.iter().map(|(order, headers)| {
        tb.message(order)
            .with_headers(headers.clone())
            .to(subject)
            .publish()
    }))
    .await
    {
        result.expect("publish");
    }
    let seen: Vec<Order> = tb.broker::<MemoryBroker>().subscriber(subject).received();
    for band in [0, BETA_BAND] {
        let ids: Vec<u32> = seen
            .iter()
            .map(|order| order.id)
            .filter(|id| (*id > band) && (*id <= band + PER_KEY))
            .collect();
        assert_eq!(
            ids.len(),
            PER_KEY as usize,
            "the whole band arrives: {ids:?}"
        );
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "per-key order lost in band {band} on {subject}: {ids:?}",
        );
    }
}

/// Passes the barrier only with three deliveries in flight at once. It names no threads of its
/// own, so only the router chain can give it any.
#[subscriber("routed")]
async fn routed(_job: &Order, ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    HandlerOutcome::ack()
}

/// A router chain gives a subscription dedicated threads through `Workers::threads`; under the
/// harness they run as that many workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_chain_gives_threads_through_workers() {
    let router = Router::<MemoryBroker>::new()
        .include(routed)
        .workers(Workers::threads(nonzero!(3)));
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Barrier::new(3))))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup");
    let jobs: Vec<Order> = (1..=3).map(|id| Order { id }).collect();
    let published = tokio::time::timeout(
        CONCURRENCY_DEADLINE,
        join_all(
            jobs.iter()
                .map(|job| tb.message(job).to("routed").publish()),
        ),
    )
    .await
    .expect("the router's threads must hold three deliveries in flight at once");
    for result in published {
        result.expect("publish");
    }
    tb.broker::<MemoryBroker>()
        .subscriber("routed")
        .assert_called(3)
        .settled(HandlerOutcome::ack());
}

/// Names no threads of its own; the router chain keys them.
#[subscriber("routed.keyed")]
async fn routed_keyed(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    tokio::task::yield_now().await;
    HandlerOutcome::ack()
}

/// A router chain keys a subscription's threads through `Workers::threads_keyed`, so each key
/// keeps its order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_router_chain_keys_threads_through_workers() {
    let router = Router::<MemoryBroker>::new()
        .include(routed_keyed)
        .workers(Workers::threads_keyed(nonzero!(4)));
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup");
    keys_stay_in_order(&tb, "routed.keyed").await;
}

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The orders the deferring handler has seen once.
#[derive(Default)]
struct Seen(Mutex<Vec<u32>>);

impl Seen {
    /// Records `id` and reports whether this is its first sighting.
    fn first(&self, id: u32) -> bool {
        let mut seen = self.0.lock().expect("unpoisoned");
        let first = !seen.contains(&id);
        if first {
            seen.push(id);
        }
        first
    }
}

/// Defers the first sight of an order, acks its redelivery.
#[subscriber("deferred", threads(2))]
async fn defer(order: &Order, ctx: &mut Context<'_, (), Arc<Seen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Under the harness the threads' redeliveries wait for the harness clock, as every timer the
/// app arms does.
#[tokio::test(start_paused = true)]
async fn redeliveries_from_threads_follow_the_harness_clock() {
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Seen::default())))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(defer);
        });
    let tb = TestApp::start(app).await.expect("startup");
    tb.message(&Order { id: 1 })
        .to("deferred")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("deferred")
        .assert_called(1);
    tb.advance(RETRY_DELAY).await.expect("settle");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("deferred")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
}
