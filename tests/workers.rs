//! Integration tests for the workers(..) dispatch policies: concurrent pools, per-key lanes,
//! and batch pools.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::handler_signal;
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, Context, HandlerMetadata, HandlerResult, Router, RustStream, Workers, typed,
};
use ruststream::{Headers, Name, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::{Barrier, Notify};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

static WARMED: AtomicU32 = AtomicU32::new(0);
static WARMED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static CRUNCHED: AtomicU32 = AtomicU32::new(0);
static CRUNCHED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static GATE: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(4));

/// Four deliveries must be in flight at once to pass the barrier; a sequential loop would
/// deadlock on the first one.
#[subscriber("jobs", workers(4))]
async fn crunch(job: &Order) -> HandlerResult {
    if job.id == 0 {
        WARMED.fetch_add(1, Ordering::SeqCst);
        WARMED_NOTIFY.notify_one();
        return HandlerResult::Ack;
    }
    GATE.wait().await;
    CRUNCHED.fetch_add(1, Ordering::SeqCst);
    CRUNCHED_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_processes_deliveries_concurrently() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| b.include(crunch));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Warm up until the subscription is live, then submit exactly the barrier's worth of jobs.
    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("jobs", &order_bytes(0)))
                .await;
            handler_signal(&WARMED_NOTIFY).await;
            if WARMED.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    for id in 1..=4u32 {
        publisher
            .publish(OutgoingMessage::new("jobs", &order_bytes(id)))
            .await
            .unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while CRUNCHED.load(Ordering::SeqCst) < 4 {
            CRUNCHED_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "4 deliveries never ran concurrently (sequential dispatch would deadlock the barrier)",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static KEYED_SEEN: Mutex<Vec<(String, u32)>> = Mutex::new(Vec::new());
static KEYED_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

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
    KEYED_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn by_key_lanes_preserve_per_key_order() {
    const PER_KEY: u32 = 10;

    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("keyed", "0.1.0")).with_broker(broker, |b| b.include(keyed));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let keyed_publish = |key: &'static str, id: u32| {
        let publisher = publisher.clone();
        async move {
            let mut headers = Headers::new();
            headers.insert("partition-key", key);
            publisher
                .publish(OutgoingMessage::new("keyed", &order_bytes(id)).with_headers(headers))
                .await
        }
    };

    // Warm up until the subscription is live (warmup deliveries carry their own key).
    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = keyed_publish("warmup", 0).await;
            handler_signal(&KEYED_NOTIFY).await;
            if !KEYED_SEEN.lock().unwrap().is_empty() {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    for id in 1..=PER_KEY {
        keyed_publish("alpha", id).await.unwrap();
        keyed_publish("beta", id + 100).await.unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            KEYED_NOTIFY.notified().await;
            let counted = KEYED_SEEN
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key == "alpha" || key == "beta")
                .count();
            if counted >= (PER_KEY as usize) * 2 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "keyed deliveries did not all arrive");

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

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static PAGES: AtomicUsize = AtomicUsize::new(0);
static PAGES_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Batch form composing with a pool: up to two pages in flight.
#[subscriber(batch("pages"), workers(2))]
async fn settle(orders: &[Order]) -> HandlerResult {
    PAGES.fetch_add(orders.len(), Ordering::SeqCst);
    PAGES_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_dispatches_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pages", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(settle));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pages", &order_bytes(1)))
                .await;
            handler_signal(&PAGES_NOTIFY).await;
            if PAGES.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "no batch was dispatched through the pool");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

/// The functional-path pool: a `Router::subscribe` closure with `.workers(Workers::pool(3))`.
/// Three deliveries must be in flight at once to pass the barrier; the default sequential loop
/// would deadlock on the first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_subscription_pool_runs_concurrently() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let warmed = Arc::new(AtomicU32::new(0));
    let crunched = Arc::new(AtomicU32::new(0));
    let signal = Arc::new(Notify::new());
    let gate = Arc::new(Barrier::new(3));

    let handler = {
        let warmed = Arc::clone(&warmed);
        let crunched = Arc::clone(&crunched);
        let signal = Arc::clone(&signal);
        let gate = Arc::clone(&gate);
        typed(JsonCodec, move |order: &Order, _ctx: &mut Context| {
            let warmed = Arc::clone(&warmed);
            let crunched = Arc::clone(&crunched);
            let signal = Arc::clone(&signal);
            let gate = Arc::clone(&gate);
            let id = order.id;
            async move {
                if id == 0 {
                    warmed.fetch_add(1, Ordering::SeqCst);
                    signal.notify_one();
                    return HandlerResult::Ack;
                }
                gate.wait().await;
                crunched.fetch_add(1, Ordering::SeqCst);
                signal.notify_one();
                HandlerResult::Ack
            }
        })
    };

    let router = Router::<MemoryBroker>::new()
        .subscribe(
            Name::new("fn-jobs"),
            handler,
            HandlerMetadata::raw("fn-jobs"),
        )
        .workers(Workers::pool(3));

    let app = RustStream::new(AppInfo::new("fn-jobs", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("fn-jobs", &order_bytes(0)))
                .await;
            handler_signal(&signal).await;
            if warmed.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    for id in 1..=3u32 {
        publisher
            .publish(OutgoingMessage::new("fn-jobs", &order_bytes(id)))
            .await
            .unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while crunched.load(Ordering::SeqCst) < 3 {
            signal.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "3 deliveries never ran concurrently (sequential dispatch would deadlock the barrier)",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

/// The functional batch path: a `Router::subscribe_batch` slice closure receives whole decoded
/// batches without a macro definition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_batch_subscription_receives_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen = Arc::new(AtomicUsize::new(0));
    let signal = Arc::new(Notify::new());

    let handler = {
        let seen = Arc::clone(&seen);
        let signal = Arc::clone(&signal);
        move |orders: &[Order], _ctx: &mut Context| {
            let count = orders.len();
            let seen = Arc::clone(&seen);
            let signal = Arc::clone(&signal);
            async move {
                seen.fetch_add(count, Ordering::SeqCst);
                signal.notify_one();
                HandlerResult::Ack
            }
        }
    };

    let router = Router::<MemoryBroker>::new()
        .subscribe_batch(
            Name::new("fn-pages"),
            handler,
            HandlerMetadata::raw("fn-pages"),
        )
        .workers(Workers::pool(2));

    let app = RustStream::new(AppInfo::new("fn-pages", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("fn-pages", &order_bytes(1)))
                .await;
            handler_signal(&signal).await;
            if seen.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "no batch reached the slice closure");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
