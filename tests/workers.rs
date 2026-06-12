//! Integration tests for the workers(..) dispatch policies: concurrent pools, per-key lanes,
//! and batch pools.
#![cfg(feature = "macros")]

use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::{Headers, OutgoingMessage, Publisher, subscriber};
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
static CRUNCHED: AtomicU32 = AtomicU32::new(0);
static GATE: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(4));

/// Four deliveries must be in flight at once to pass the barrier; a sequential loop would
/// deadlock on the first one.
#[subscriber("jobs", workers(4))]
async fn crunch(job: &Order) -> HandlerResult {
    if job.id == 0 {
        WARMED.fetch_add(1, Ordering::SeqCst);
        return HandlerResult::Ack;
    }
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

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Warm up until the subscription is live, then submit exactly the barrier's worth of jobs.
    let warmup = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("jobs", &order_bytes(0)))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
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

    let result = tokio::time::timeout(Duration::from_secs(2), async {
        while CRUNCHED.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
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
    let warmup = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = keyed_publish("warmup", 0).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
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

    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
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

    let app = RustStream::new(AppInfo::new("pages", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(settle));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pages", &order_bytes(1)))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
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
