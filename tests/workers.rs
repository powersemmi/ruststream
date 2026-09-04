//! Integration tests for the workers(..) dispatch policies: concurrent pools, per-key lanes,
//! and batch pools.
//!
//! The pool tests inject their deliveries together rather than one at a time: a pool only has
//! something to spread over its workers while more than one delivery is in flight, and the
//! harness settles the whole reaction before the injections resolve.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::{
    future::{Future, ready},
    sync::Arc,
    time::Duration,
};

use common::Order;
use futures::future::join_all;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use tokio::sync::Barrier;

/// The deadline every "did the pool run these together?" wait rides. A pool that dispatched
/// sequentially would park on the barrier forever, so the timeout is what turns that deadlock
/// into a failed assertion.
const CONCURRENCY_DEADLINE: Duration = Duration::from_secs(5);

/// Four deliveries must be in flight at once to pass the barrier; a sequential loop would
/// deadlock on the first one.
#[subscriber("jobs", workers(4))]
async fn crunch(_job: &Order, ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_processes_deliveries_concurrently() {
    let app = RustStream::new(AppInfo::new("jobs", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Arc::new(Barrier::new(4))))
        .with_broker(MemoryBroker::new(), |b| b.include(crunch));
    let tb = TestApp::start(app).await.expect("startup failed");

    // Exactly the barrier's worth of jobs: dispatched sequentially, the first would park on the
    // barrier and the deadline below would expire.
    let jobs: Vec<Order> = (1..=4u32).map(|id| Order { id }).collect();
    let published = tokio::time::timeout(
        CONCURRENCY_DEADLINE,
        join_all(jobs.iter().map(|job| tb.message(job).to("jobs").publish())),
    )
    .await
    .expect("the pool must hold four deliveries in flight at once");
    for result in published {
        result.expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called(4)
        .settled(HandlerOutcome::ack());
}

/// Records nothing of its own: the id carries the key, so the harness's delivery order per key
/// is what the assertion reads.
#[subscriber("keyed", workers(4, by_key))]
async fn keyed(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    // Encourage interleaving between lanes; each lane itself stays sequential.
    tokio::task::yield_now().await;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn by_key_lanes_preserve_per_key_order() {
    const PER_KEY: u32 = 10;
    // The key rides the headers (that is what picks the lane) and the id band says which key a
    // delivery belongs to, so per-key order is readable off the recorded deliveries alone.
    const BETA_BAND: u32 = 100;

    let app = RustStream::new(AppInfo::new("keyed", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(keyed));
    let tb = TestApp::start(app).await.expect("startup failed");

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

    // Injected together, in publish order, so the lanes have a stream to keep in order.
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
            "the whole key band must arrive: {ids:?}",
        );
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "per-key order lost in band {band}: {ids:?}",
        );
    }
}

/// Batch form composing with a pool: up to two pages in flight.
#[subscriber("pages", workers(2))]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_dispatches_batches() {
    let app = RustStream::new(AppInfo::new("pages", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(settle.batch(nonzero!(8))));
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("pages")
        .publish()
        .await
        .expect("publish");

    // A batch carrying the message must be dispatched through the pool.
    tb.broker::<MemoryBroker>()
        .subscriber("pages")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
}

/// The manual path's body of the pool test: it passes the barrier only if the requested number of
/// deliveries is in flight at once.
struct CrunchJobs {
    gate: Arc<Barrier>,
}

impl Handle<Order> for CrunchJobs {
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

/// The manual-path pool: a `subscriber(..)` definition with `.workers(Workers::pool(nonzero!(3)))`
/// named on the router. Three deliveries must be in flight at once to pass the barrier; the
/// default sequential loop would deadlock on the first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_subscription_pool_runs_concurrently() {
    let handler = CrunchJobs {
        gate: Arc::new(Barrier::new(3)),
    };

    let router = Router::<MemoryBroker>::new()
        .include(subscriber("fn-jobs", handler).build())
        .workers(Workers::pool(nonzero!(3)));

    let app = RustStream::new(AppInfo::new("fn-jobs", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    let jobs: Vec<Order> = (1..=3u32).map(|id| Order { id }).collect();
    let published = tokio::time::timeout(
        CONCURRENCY_DEADLINE,
        join_all(
            jobs.iter()
                .map(|job| tb.message(job).to("fn-jobs").publish()),
        ),
    )
    .await
    .expect("the pool must hold three deliveries in flight at once");
    for result in published {
        result.expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .subscriber("fn-jobs")
        .assert_called(3)
        .settled(HandlerOutcome::ack());
}

/// The manual path's page body: it takes whole decoded pages, so the batch either arrived as a
/// page or did not.
struct CountPages;

impl Handle<[Order]> for CountPages {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = orders;
        ready(Ok(()))
    }
}

/// The manual batch path: a page body receives whole decoded batches without a macro definition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_batch_subscription_receives_batches() {
    let router = Router::<MemoryBroker>::new()
        .include(
            subscriber("fn-pages", CountPages)
                .batch(nonzero!(8))
                .build(),
        )
        .workers(Workers::pool(nonzero!(2)));

    let app = RustStream::new(AppInfo::new("fn-pages", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("fn-pages")
        .publish()
        .await
        .expect("publish");

    // The message must reach the slice body as a decoded batch.
    tb.broker::<MemoryBroker>()
        .subscriber("fn-pages")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
}
