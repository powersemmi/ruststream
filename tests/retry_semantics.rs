//! Integration tests for retry semantics at the dispatcher level: the `retry_after` delay is
//! honored (not merely "redelivery happens"), retries complete inside worker pools and keyed
//! lanes, and batch pools genuinely overlap batches.
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so every
//! message is published exactly once; any further delivery is a genuine redelivery.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::{Order, order_bytes};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::subscriber;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

const RETRY_DELAY: Duration = Duration::from_secs(5);

static DELAY_ATTEMPTS: Mutex<Vec<Instant>> = Mutex::new(Vec::new());
static DELAY_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Records the paused-clock instant of every attempt; defers the first one by `RETRY_DELAY`.
#[subscriber("delayed")]
async fn deferred(_o: &Order) -> HandlerOutcome {
    let mut attempts = DELAY_ATTEMPTS.lock().unwrap();
    attempts.push(Instant::now());
    let first = attempts.len() == 1;
    drop(attempts);
    DELAY_NOTIFY.notify_one();
    if first {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
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

    let running = app.start().await.expect("startup failed");

    // One publish is enough: the second attempt must come from the delayed redelivery.
    publisher
        .raw(&order_bytes(1))
        .to("delayed")
        .publish()
        .await
        .expect("publish");

    let result = tokio::time::timeout(Duration::from_secs(60), async {
        while DELAY_ATTEMPTS.lock().unwrap().len() < 2 {
            DELAY_NOTIFY.notified().await;
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

    running.shutdown().await.expect("graceful shutdown failed");
}

static POOL_ACKED: AtomicU32 = AtomicU32::new(0);
static POOL_RETRIED: AtomicU32 = AtomicU32::new(0);
static POOL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// First sight of each id asks for an immediate retry; the redelivery is acked.
#[subscriber("pool-retry", workers(3))]
async fn pool_retry(order: &Order) -> HandlerOutcome {
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
        return HandlerOutcome::retry();
    }
    POOL_ACKED.fetch_add(1, Ordering::SeqCst);
    POOL_NOTIFY.notify_one();
    HandlerOutcome::ack()
}

/// Retried deliveries re-enter a worker pool and complete: every message is nacked once
/// (requeue) and acked on the second pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_completes_inside_a_worker_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pool-retry", "0.1.0"))
        .with_broker(broker, |b| b.include(pool_retry));

    let running = app.start().await.expect("startup failed");

    for id in 1..=4u32 {
        publisher
            .raw(&order_bytes(id))
            .to("pool-retry")
            .publish()
            .await
            .expect("publish");
    }

    // 4 distinct ids, each retried once then acked.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while POOL_ACKED.load(Ordering::SeqCst) < 4 {
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

    running.shutdown().await.expect("graceful shutdown failed");
}

static LANE_ACKED: AtomicU32 = AtomicU32::new(0);
static LANE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// First sight of each id asks for an immediate retry; the redelivery is acked.
#[subscriber("lane-retry", workers(2, by_key))]
async fn lane_retry(order: &Order) -> HandlerOutcome {
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
        return HandlerOutcome::retry();
    }
    LANE_ACKED.fetch_add(1, Ordering::SeqCst);
    LANE_NOTIFY.notify_one();
    HandlerOutcome::ack()
}

/// Retried deliveries re-enter keyed lanes and complete (per-key ordering across a retry is
/// not promised: a requeued message rejoins the stream from the back).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_completes_inside_keyed_lanes() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("lane-retry", "0.1.0"))
        .with_broker(broker, |b| b.include(lane_retry));

    let running = app.start().await.expect("startup failed");

    let keyed_publish = |key: &'static str, id: u32| {
        let publisher = publisher.clone();
        async move {
            let mut headers = ruststream::HeaderMap::new();
            headers.insert("partition-key", key);
            publisher
                .raw(&order_bytes(id))
                .with_headers(headers)
                .to("lane-retry")
                .publish()
                .await
        }
    };

    // Two keyed messages, each retried once then acked.
    keyed_publish("alpha", 1).await.expect("publish");
    keyed_publish("beta", 2).await.expect("publish");

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while LANE_ACKED.load(Ordering::SeqCst) < 2 {
            LANE_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "retried deliveries did not all complete in keyed lanes: acked {}",
        LANE_ACKED.load(Ordering::SeqCst),
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

static BATCHES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static OVERLAP_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
/// Flipping this to `true` releases every held and future batch, so no handler task can outlive
/// the test and hang the graceful drain (a barrier would strand a third, unpaired batch).
static RELEASE: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));

/// Holds every batch until the test observes two of them in flight at once.
#[subscriber("overlap", workers(2))]
async fn overlap(_orders: &[Order]) -> HandlerOutcome {
    BATCHES_IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    OVERLAP_NOTIFY.notify_one();
    let mut release = RELEASE.subscribe();
    let _ = release.wait_for(|released| *released).await;
    BATCHES_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// A batch pool genuinely overlaps batches: with `workers(2)`, a second batch is pulled and
/// dispatched while the first is still being handled (both sit on the release latch at once).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_overlaps_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("overlap", "0.1.0"))
        .with_broker(broker, |b| b.include(overlap));

    let running = app.start().await.expect("startup failed");

    // Publish the second message only after the first batch is held on the latch, so the two
    // messages arrive in distinct batches - which a sequential batch loop could never hold in
    // flight simultaneously.
    publisher
        .raw(&order_bytes(1))
        .to("overlap")
        .publish()
        .await
        .expect("publish");
    let first_held = tokio::time::timeout(Duration::from_secs(5), async {
        while BATCHES_IN_FLIGHT.load(Ordering::SeqCst) < 1 {
            OVERLAP_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(first_held.is_ok(), "the first batch never reached the pool");

    publisher
        .raw(&order_bytes(2))
        .to("overlap")
        .publish()
        .await
        .expect("publish");
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while BATCHES_IN_FLIGHT.load(Ordering::SeqCst) < 2 {
            OVERLAP_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "two batches were never in flight at once through the pool",
    );

    // Release every held (and any late) batch so the graceful drain can finish.
    RELEASE.send_replace(true);
    running.shutdown().await.expect("graceful shutdown failed");
}
