//! Integration tests for retry semantics at the dispatcher level: the `retry_after` delay is
//! honored (not merely "redelivery happens"), retries complete inside worker pools and keyed
//! lanes, and batch pools genuinely overlap batches.
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
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

const RETRY_DELAY: Duration = Duration::from_secs(5);

static DELAY_ATTEMPTS: Mutex<Vec<Instant>> = Mutex::new(Vec::new());
static DELAY_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Records the paused-clock instant of every attempt; defers the first one by `RETRY_DELAY`.
#[subscriber("delayed")]
async fn deferred(_o: &Order) -> HandlerResult {
    let mut attempts = DELAY_ATTEMPTS.lock().unwrap();
    attempts.push(Instant::now());
    let first = attempts.len() == 1;
    drop(attempts);
    DELAY_NOTIFY.notify_one();
    if first {
        HandlerResult::retry_after(RETRY_DELAY)
    } else {
        HandlerResult::Ack
    }
}

/// The dispatcher must hold the redelivery back for the full `retry_after` delay, measured on
/// the paused clock - not merely redeliver eventually.
///
/// `start_paused` requires the current-thread runtime (tokio cannot pause a multithreaded
/// clock); the auto-advancing timer makes the test instant while keeping the measured interval
/// exact.
#[tokio::test(start_paused = true)]
async fn retry_after_delay_is_honored_by_the_dispatcher() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("delayed", "0.1.0"))
        .with_broker(broker, |b| b.include(deferred));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let payload = order_bytes(1);
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            // One delivery is enough once the subscription is live: the second attempt must
            // come from the delayed redelivery, never from this loop.
            if DELAY_ATTEMPTS.lock().unwrap().is_empty() {
                let _ = publisher
                    .publish(OutgoingMessage::new("delayed", &payload))
                    .await;
            }
            handler_signal(&DELAY_NOTIFY).await;
            if DELAY_ATTEMPTS.lock().unwrap().len() >= 2 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "the deferred message was never redelivered");

    let between = {
        let attempts = DELAY_ATTEMPTS.lock().unwrap();
        attempts[1].duration_since(attempts[0])
    };
    assert!(
        between >= RETRY_DELAY,
        "redelivery arrived after {between:?}, before the requested {RETRY_DELAY:?}",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static POOL_ACKED: AtomicU32 = AtomicU32::new(0);
static POOL_RETRIED: AtomicU32 = AtomicU32::new(0);
static POOL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// First sight of each id asks for an immediate retry; the redelivery is acked.
#[subscriber("pool-retry", workers(3))]
async fn pool_retry(order: &Order) -> HandlerResult {
    static FIRST_SEEN: Mutex<Vec<u32>> = Mutex::new(Vec::new());
    let first = {
        let mut seen = FIRST_SEEN.lock().unwrap();
        if seen.contains(&order.id) {
            false
        } else {
            seen.push(order.id);
            true
        }
    };
    if first {
        POOL_RETRIED.fetch_add(1, Ordering::SeqCst);
        POOL_NOTIFY.notify_one();
        return HandlerResult::retry();
    }
    POOL_ACKED.fetch_add(1, Ordering::SeqCst);
    POOL_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// Retried deliveries re-enter a worker pool and complete: every message is nacked once
/// (requeue) and acked on the second pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_completes_inside_a_worker_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pool-retry", "0.1.0"))
        .with_broker(broker, |b| b.include(pool_retry));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Warm up with id 0 until the subscription is live, then submit ids 1-4 exactly once.
    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pool-retry", &order_bytes(0)))
                .await;
            handler_signal(&POOL_NOTIFY).await;
            if POOL_RETRIED.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    for id in 1..=4u32 {
        publisher
            .publish(OutgoingMessage::new("pool-retry", &order_bytes(id)))
            .await
            .unwrap();
    }

    // 5 distinct ids (warmup included), each retried once then acked.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while POOL_ACKED.load(Ordering::SeqCst) < 5 {
            POOL_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "retried deliveries did not all complete in the pool: acked {}, retried {}",
        POOL_ACKED.load(Ordering::SeqCst),
        POOL_RETRIED.load(Ordering::SeqCst),
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static LANE_ACKED: AtomicU32 = AtomicU32::new(0);
static LANE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// First sight of each id asks for an immediate retry; the redelivery is acked.
#[subscriber("lane-retry", workers(2, by_key))]
async fn lane_retry(order: &Order) -> HandlerResult {
    static FIRST_SEEN: Mutex<Vec<u32>> = Mutex::new(Vec::new());
    let first = {
        let mut seen = FIRST_SEEN.lock().unwrap();
        if seen.contains(&order.id) {
            false
        } else {
            seen.push(order.id);
            true
        }
    };
    LANE_NOTIFY.notify_one();
    if first {
        return HandlerResult::retry();
    }
    LANE_ACKED.fetch_add(1, Ordering::SeqCst);
    LANE_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// Retried deliveries re-enter keyed lanes and complete (per-key ordering across a retry is
/// not promised: a requeued message rejoins the stream from the back).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_completes_inside_keyed_lanes() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("lane-retry", "0.1.0"))
        .with_broker(broker, |b| b.include(lane_retry));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let keyed_publish = |key: &'static str, id: u32| {
        let publisher = publisher.clone();
        async move {
            let mut headers = ruststream::Headers::new();
            headers.insert("partition-key", key);
            publisher
                .publish(OutgoingMessage::new("lane-retry", &order_bytes(id)).with_headers(headers))
                .await
        }
    };

    // Warm up with id 0 until the subscription is live (its retry also completes), then two
    // keyed messages, each retried once.
    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = keyed_publish("warmup", 0).await;
            handler_signal(&LANE_NOTIFY).await;
            if LANE_ACKED.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    keyed_publish("alpha", 1).await.unwrap();
    keyed_publish("beta", 2).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while LANE_ACKED.load(Ordering::SeqCst) < 3 {
            LANE_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "retried deliveries did not all complete in keyed lanes: acked {}",
        LANE_ACKED.load(Ordering::SeqCst),
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static BATCHES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static OVERLAP_WARMED: AtomicUsize = AtomicUsize::new(0);
static OVERLAP_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
/// Flipping this to `true` releases every held and future batch, so no handler task can outlive
/// the test and hang the graceful drain (a barrier would strand a third, unpaired batch).
static RELEASE: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));

/// Holds every non-warmup batch until the test observes two of them in flight at once.
#[subscriber(batch("overlap"), workers(2))]
async fn overlap(orders: &[Order]) -> HandlerResult {
    if orders.iter().any(|o| o.id == 0) {
        OVERLAP_WARMED.fetch_add(1, Ordering::SeqCst);
        OVERLAP_NOTIFY.notify_one();
        return HandlerResult::Ack;
    }
    BATCHES_IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    OVERLAP_NOTIFY.notify_one();
    let mut release = RELEASE.subscribe();
    let _ = release.wait_for(|released| *released).await;
    BATCHES_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// A batch pool genuinely overlaps batches: with `workers(2)`, a second batch is pulled and
/// dispatched while the first is still being handled (both sit on the release latch at once).
/// The existing smoke test only proved a batch arrives through the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_overlaps_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("overlap", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(overlap));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Warm up until the subscription is live.
    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("overlap", &order_bytes(0)))
                .await;
            handler_signal(&OVERLAP_NOTIFY).await;
            if OVERLAP_WARMED.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    // Keep publishing single messages; held batches accumulate on the latch until two are in
    // flight simultaneously - which a sequential batch loop can never reach.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut id = 1u32;
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("overlap", &order_bytes(id)))
                .await;
            id += 1;
            handler_signal(&OVERLAP_NOTIFY).await;
            if BATCHES_IN_FLIGHT.load(Ordering::SeqCst) >= 2 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "two batches were never in flight at once through the pool",
    );

    // Release every held (and any late) batch so the graceful drain can finish.
    RELEASE.send_replace(true);
    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
