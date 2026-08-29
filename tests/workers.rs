//! Integration tests for the workers(..) dispatch policies: concurrent pools, per-key lanes,
//! and batch pools.
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so every
//! message is published exactly once - no warmup or republish loops.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::{Order, order_bytes, wait_for};
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use tokio::sync::Barrier;

static CRUNCHED: AtomicU32 = AtomicU32::new(0);
static GATE: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(4));

/// Four deliveries must be in flight at once to pass the barrier; a sequential loop would
/// deadlock on the first one.
#[subscriber("jobs", workers(4))]
async fn crunch(_job: &Order) -> HandlerResult {
    GATE.wait().await;
    CRUNCHED.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_processes_deliveries_concurrently() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| b.include(crunch));

    let running = app.start().await.expect("startup failed");

    // Exactly the barrier's worth of jobs: if they were dispatched sequentially, the first one
    // would deadlock on the barrier and the wait below would time out.
    for id in 1..=4u32 {
        publisher
            .raw(&order_bytes(id))
            .to("jobs")
            .publish()
            .await
            .expect("publish");
    }

    wait_for(
        || CRUNCHED.load(Ordering::SeqCst) >= 4,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static KEYED_SEEN: Mutex<Vec<(String, u32)>> = Mutex::new(Vec::new());

/// Records (key, id) pairs; per-key arrival order must match publish order.
#[subscriber("keyed", workers(4, by_key))]
async fn keyed(order: &Order, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerResult {
    let key = ctx
        .headers()
        .get_str("partition-key")
        .unwrap_or_default()
        .to_owned();
    // Encourage interleaving between lanes; each lane itself stays sequential.
    tokio::task::yield_now().await;
    KEYED_SEEN.lock().unwrap().push((key, order.id));
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn by_key_lanes_preserve_per_key_order() {
    const PER_KEY: u32 = 10;

    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("keyed", "0.1.0")).with_broker(broker, |b| b.include(keyed));

    let running = app.start().await.expect("startup failed");

    let keyed_publish = |key: &'static str, id: u32| {
        let publisher = publisher.clone();
        async move {
            let mut headers = HeaderMap::new();
            headers.insert("partition-key", key);
            publisher
                .raw(&order_bytes(id))
                .with_headers(headers)
                .to("keyed")
                .publish()
                .await
        }
    };

    for id in 1..=PER_KEY {
        keyed_publish("alpha", id).await.expect("publish");
        keyed_publish("beta", id + 100).await.expect("publish");
    }

    wait_for(
        || KEYED_SEEN.lock().unwrap().len() >= (PER_KEY as usize) * 2,
        Duration::from_secs(5),
    )
    .await;

    let seen = KEYED_SEEN.lock().unwrap().clone();
    for key in ["alpha", "beta"] {
        let ids: Vec<u32> = seen
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, id)| *id)
            .collect();
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "per-key order lost for {key}: {ids:?}",
        );
    }

    running.shutdown().await.expect("graceful shutdown failed");
}

static PAGES: AtomicUsize = AtomicUsize::new(0);

/// Batch form composing with a pool: up to two pages in flight.
#[subscriber(batch("pages"), workers(2))]
async fn settle(orders: &[Order]) -> HandlerResult {
    PAGES.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_dispatches_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("pages", "0.1.0")).with_broker(broker, |b| b.include(settle));

    let running = app.start().await.expect("startup failed");

    publisher
        .raw(&order_bytes(1))
        .to("pages")
        .publish()
        .await
        .expect("publish");

    // A batch carrying the message must be dispatched through the pool.
    wait_for(|| PAGES.load(Ordering::SeqCst) >= 1, Duration::from_secs(5)).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The functional-path pool: a `subscriber(..)` closure with `.workers(Workers::pool(nonzero!(3)))`
/// named on the router. Three deliveries must be in flight at once to pass the barrier; the
/// default sequential loop would deadlock on the first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_subscription_pool_runs_concurrently() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let crunched = Arc::new(AtomicU32::new(0));
    let gate = Arc::new(Barrier::new(3));

    let handler = {
        let crunched = Arc::clone(&crunched);
        let gate = Arc::clone(&gate);
        move |_order: &Order, _ctx: &mut Context| {
            let crunched = Arc::clone(&crunched);
            let gate = Arc::clone(&gate);
            async move {
                gate.wait().await;
                crunched.fetch_add(1, Ordering::SeqCst);
                HandlerResult::Ack
            }
        }
    };

    let router = Router::<MemoryBroker>::new()
        .include(subscriber("fn-jobs", handler))
        .workers(Workers::pool(nonzero!(3)));

    let app = RustStream::new(AppInfo::new("fn-jobs", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    // Exactly the barrier's worth of jobs: sequential dispatch would deadlock on the first one
    // and the wait below would time out.
    for id in 1..=3u32 {
        publisher
            .raw(&order_bytes(id))
            .to("fn-jobs")
            .publish()
            .await
            .expect("publish");
    }

    wait_for(
        || crunched.load(Ordering::SeqCst) >= 3,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// The functional batch path: a `batch(..)` slice closure receives whole decoded batches without
/// a macro definition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_batch_subscription_receives_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen = Arc::new(AtomicUsize::new(0));

    let handler = {
        let seen = Arc::clone(&seen);
        move |orders: &[Order], _ctx: &mut Context| {
            let count = orders.len();
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(count, Ordering::SeqCst);
                HandlerResult::Ack
            }
        }
    };

    let router = Router::<MemoryBroker>::new()
        .include(batch("fn-pages", handler))
        .workers(Workers::pool(nonzero!(2)));

    let app = RustStream::new(AppInfo::new("fn-pages", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    publisher
        .raw(&order_bytes(1))
        .to("fn-pages")
        .publish()
        .await
        .expect("publish");

    // The message must reach the slice closure as a decoded batch.
    wait_for(|| seen.load(Ordering::SeqCst) >= 1, Duration::from_secs(5)).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
