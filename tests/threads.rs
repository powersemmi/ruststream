//! `threads(n)` under the test harness, and the handle to the app's runtime a handler reaches
//! through its context.
//!
//! `TestApp` runs every subscription on the test's own runtime, a `threads(n)` one included, so
//! these tests assert what a handler sees and how its deliveries settle, never where they ran.
//! Placement itself is the subject of `threads_placement.rs`.
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
use ruststream::{BuildContext, ContextField, HeaderMap, nonzero};
use tokio::runtime::Handle;
use tokio::sync::Barrier;

/// A pool that ran its deliveries one at a time parks on the barrier forever; the deadline turns
/// that into a failed assertion.
const CONCURRENCY_DEADLINE: Duration = Duration::from_secs(5);

/// Acks where the context's main-runtime handle is the runtime the handler runs on: under the
/// harness that is the test's own.
#[subscriber("where")]
async fn on_main(_order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    if ctx.main_runtime().id() == Handle::current().id() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_context_hands_out_the_app_runtime() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_main);
        });
    let tb = TestApp::start(app).await.expect("startup");
    tb.message(&Order { id: 1 })
        .to("where")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("where")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
}

/// The same through the extractor, with no context parameter.
#[subscriber("extracted")]
async fn extracted(_order: &Order, Ctx(main): Ctx<MainRuntime>) -> HandlerOutcome {
    if main.id() == Handle::current().id() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

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
    Ctx(main): Ctx<MainRuntime>,
    Ctx(len): Ctx<PayloadLen>,
) -> HandlerOutcome {
    if main.id() == Handle::current().id() && len > 0 {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_extractor_hands_out_the_app_runtime_beside_broker_keys() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(extracted);
            b.include(mixed);
        });
    let tb = TestApp::start(app).await.expect("startup");
    for subject in ["extracted", "mixed"] {
        tb.message(&Order { id: 1 })
            .to(subject)
            .publish()
            .await
            .expect("publish");
        tb.broker::<MemoryBroker>()
            .subscriber(subject)
            .assert_called(1)
            .settled(HandlerOutcome::ack());
    }
}

/// Passes the barrier only with four deliveries in flight at once, and acks only on the test's
/// own runtime: under the harness `threads(n)` runs as `n` workers there.
#[subscriber("crunch", threads(4))]
async fn crunch(_job: &Order, ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    if ctx.main_runtime().id() == Handle::current().id() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// The same body without the clause, for the mount-site steps.
#[subscriber("crunch")]
async fn crunch_open(_job: &Order, ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    if ctx.main_runtime().id() == Handle::current().id() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// Publishes `count` jobs together to `crunch` and asserts every one acked.
async fn crunch_concurrently(tb: &TestApp<Arc<Barrier>>, count: u32) {
    let jobs: Vec<Order> = (1..=count).map(|id| Order { id }).collect();
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
        .assert_called(count as usize)
        .settled(HandlerOutcome::ack());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_clause_runs_its_threads_as_workers_of_the_test_runtime() {
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Barrier::new(4))))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(crunch);
        });
    let tb = TestApp::start(app).await.expect("startup");
    crunch_concurrently(&tb, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_definition_step_declares_threads() {
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Barrier::new(3))))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(crunch_open.threads(nonzero!(3)));
        });
    let tb = TestApp::start(app).await.expect("startup");
    crunch_concurrently(&tb, 3).await;
}

/// The manual path's body: it passes the barrier only with the requested number of deliveries
/// in flight at once.
struct CrunchJobs {
    gate: Arc<Barrier>,
}

impl ruststream::runtime::Handle<Order> for CrunchJobs {
    async fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        self.gate.wait().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_chain_takes_threads_through_workers() {
    let router = Router::<MemoryBroker>::new()
        .include(
            subscriber(
                "crunch",
                CrunchJobs {
                    gate: Arc::new(Barrier::new(3)),
                },
            )
            .build(),
        )
        .workers(Workers::threads(nonzero!(3)));
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup");
    let jobs: Vec<Order> = (1..=3u32).map(|id| Order { id }).collect();
    let published = tokio::time::timeout(
        CONCURRENCY_DEADLINE,
        join_all(
            jobs.iter()
                .map(|job| tb.message(job).to("crunch").publish()),
        ),
    )
    .await
    .expect("three deliveries in flight at once");
    for result in published {
        result.expect("publish");
    }
    tb.broker::<MemoryBroker>()
        .subscriber("crunch")
        .assert_called(3)
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

#[subscriber("keyed")]
async fn keyed_open(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    tokio::task::yield_now().await;
    HandlerOutcome::ack()
}

async fn keys_stay_in_order(tb: &TestApp<()>) {
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
            .to("keyed")
            .publish()
    }))
    .await
    {
        result.expect("publish");
    }
    let seen: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("keyed").received();
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
            "per-key order lost in band {band}: {ids:?}",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_threads_keep_each_key_in_order() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(keyed);
        });
    let tb = TestApp::start(app).await.expect("startup");
    keys_stay_in_order(&tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_threads_by_the_definition_step_and_the_router_chain() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(keyed_open.threads_by_key(nonzero!(4)));
        });
    let tb = TestApp::start(app).await.expect("startup");
    keys_stay_in_order(&tb).await;
    tb.shutdown().await.expect("shutdown");

    let router = Router::<MemoryBroker>::new()
        .include(keyed_open)
        .workers(Workers::threads_keyed(nonzero!(4)));
    let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup");
    keys_stay_in_order(&tb).await;
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

/// A batch body on dedicated threads.
#[subscriber("batches", threads(2))]
async fn batches(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_body_runs_on_threads() {
    let app =
        RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(batches.batch(nonzero!(8)));
        });
    let tb = TestApp::start(app).await.expect("startup");
    tb.message(&Order { id: 1 })
        .to("batches")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("batches")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
}
