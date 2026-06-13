//! Integration tests for the batch subscriber pipeline: the `#[subscriber(batch(..))]` form,
//! `include_batch` mounting, per-element decode failures, and the `Buffered` adapter.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::handler_signal;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream, TypedPublisher};
use ruststream::testing::TestClient;
use ruststream::{Buffered, Name, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

static BATCHES: Mutex<Vec<Vec<u32>>> = Mutex::new(Vec::new());
static BILL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Settles a whole page of orders at once.
#[subscriber(batch("orders"))]
async fn bill(orders: &[Order]) -> HandlerResult {
    BATCHES
        .lock()
        .unwrap()
        .push(orders.iter().map(|o| o.id).collect());
    BILL_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_macro_def_receives_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(bill));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // The subscription opens inside run(); retry publishing until deliveries land.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for id in 0..3u32 {
                let _ = publisher
                    .publish(OutgoingMessage::new("orders", &order_bytes(id)))
                    .await;
            }
            handler_signal(&BILL_NOTIFY).await;
            let received: usize = BATCHES.lock().unwrap().iter().map(Vec::len).sum();
            if received >= 3 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "no batch arrived within the deadline");

    // Order within and across batches must follow publish order.
    let flattened: Vec<u32> = BATCHES.lock().unwrap().iter().flatten().copied().collect();
    assert!(flattened.starts_with(&[0, 1, 2]), "got {flattened:?}");
    assert!(
        BATCHES
            .lock()
            .unwrap()
            .iter()
            .all(|batch| !batch.is_empty()),
        "batches must not be empty",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static GOOD_IDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static SIFT_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Records the ids that survived decoding.
#[subscriber(batch("mixed"))]
async fn sift(orders: &[Order]) -> HandlerResult {
    GOOD_IDS.lock().unwrap().extend(orders.iter().map(|o| o.id));
    SIFT_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undecodable_elements_never_reach_the_handler() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(sift));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("mixed", &order_bytes(1)))
                .await;
            let _ = publisher
                .publish(OutgoingMessage::new("mixed", b"not json"))
                .await;
            let _ = publisher
                .publish(OutgoingMessage::new("mixed", &order_bytes(2)))
                .await;
            handler_signal(&SIFT_NOTIFY).await;
            // Subscriptions open inside run(), so the first publishes can be lost and the loop
            // republishes; wait until both decodable ids have actually arrived rather than for a
            // bare count, which a partial first batch would satisfy out of order.
            let both_seen = {
                let seen = GOOD_IDS.lock().unwrap();
                seen.contains(&1) && seen.contains(&2)
            };
            if both_seen {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "decodable elements did not arrive");

    // The undecodable element is dropped individually, never failing the batch around it: only the
    // two decodable ids ever reach the handler (the loop above already confirmed both did).
    let ids = GOOD_IDS.lock().unwrap().clone();
    assert!(
        ids.iter().all(|&id| id == 1 || id == 2),
        "an undecodable element reached the handler: {ids:?}"
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static BUFFERED_SEEN: AtomicUsize = AtomicUsize::new(0);
static DRAIN_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// A handler mounted on a `Buffered`-wrapped source directly in the macro. The macro recovers
/// the source type from the constructor path, so a generic source spells its parameter
/// (turbofish).
#[subscriber(batch(Buffered::<Name>::new(Name::new("events")).max_size(2)))]
async fn drain(events: &[Order]) -> HandlerResult {
    BUFFERED_SEEN.fetch_add(events.len(), Ordering::SeqCst);
    DRAIN_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_adapter_batches_plain_subscribers_via_router() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // Mounted through the Router path to cover include_batch there as well.
    let router = Router::<MemoryBroker>::new().include_batch(drain);
    let app = RustStream::new(AppInfo::new("events", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("events", &order_bytes(7)))
                .await;
            handler_signal(&DRAIN_NOTIFY).await;
            if BUFFERED_SEEN.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "buffered batch did not arrive");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static SETTLED: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static RECONCILE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static RETRIED_ONCE: AtomicBool = AtomicBool::new(false);

/// Retries order 11 on first sight; settles everything else, per element.
#[subscriber(batch("pages"))]
async fn reconcile(orders: &[Order]) -> Vec<HandlerResult> {
    let results = orders
        .iter()
        .map(|o| {
            if o.id == 11 && !RETRIED_ONCE.swap(true, Ordering::SeqCst) {
                HandlerResult::retry()
            } else {
                SETTLED.lock().unwrap().push(o.id);
                HandlerResult::Ack
            }
        })
        .collect();
    RECONCILE_NOTIFY.notify_one();
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_element_outcomes_retry_individually() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pages", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(reconcile));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Warm up until the subscription is live, then publish the real page exactly once, so the
    // retry accounting below is deterministic.
    let warmup = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pages", &order_bytes(0)))
                .await;
            handler_signal(&RECONCILE_NOTIFY).await;
            if SETTLED.lock().unwrap().contains(&0) {
                break;
            }
        }
    })
    .await;
    assert!(warmup.is_ok(), "subscription did not come up");

    for id in [10u32, 11, 12] {
        publisher
            .publish(OutgoingMessage::new("pages", &order_bytes(id)))
            .await
            .unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            handler_signal(&RECONCILE_NOTIFY).await;
            let settled = SETTLED.lock().unwrap().clone();
            if [10, 11, 12].iter().all(|id| settled.contains(id)) {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "retried element was not redelivered");

    // 11 was retried exactly once and settled only on redelivery; 10 and 12 settled first try.
    assert!(RETRIED_ONCE.load(Ordering::SeqCst));
    let settled = SETTLED.lock().unwrap().clone();
    for id in [10u32, 11, 12] {
        assert_eq!(
            settled.iter().filter(|s| **s == id).count(),
            1,
            "{id} must settle exactly once; settled: {settled:?}",
        );
    }

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
struct Confirmation {
    id: u32,
    accepted: bool,
}

static BATCH_CONFIRM_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Confirms a page of orders. The Result form gives explicit ack control; the whole-batch
/// rejection path is covered by the runtime unit tests.
#[subscriber(batch("requests"), publish("confirmations"))]
async fn confirm(orders: &[Order]) -> Result<Vec<Confirmation>, HandlerResult> {
    BATCH_CONFIRM_NOTIFY.notify_one();
    Ok(orders
        .iter()
        .map(|o| Confirmation {
            id: o.id,
            accepted: true,
        })
        .collect())
}

/// The plain reply form: every page is confirmed (compile coverage for `-> Vec<Reply>`).
#[subscriber(batch("requests"), publish("audit"))]
async fn audit(orders: &[Order]) -> Vec<Confirmation> {
    orders
        .iter()
        .map(|o| Confirmation {
            id: o.id,
            accepted: true,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_replies_publish_transactionally() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let observer = broker.clone();

    let replies = TypedPublisher::new(broker.publisher()).transactional();
    let app = RustStream::new(AppInfo::new("confirmations", "0.1.0"))
        .with_broker(broker, |b| b.include_batch_publishing(confirm, replies));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Retry publishing until the subscription is live, waking as soon as the handler fires;
    // the published confirmations are then awaited with expect_published's own deadline.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("requests", &order_bytes(7)))
                .await;
            handler_signal(&BATCH_CONFIRM_NOTIFY).await;
            let confirmed = observer
                .expect_published("confirmations", 1, Duration::from_millis(200))
                .await
                .unwrap();
            if !confirmed.is_empty() {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "no confirmation arrived");

    let confirmed = observer
        .expect_published("confirmations", 1, Duration::from_millis(100))
        .await
        .unwrap();
    for raw in &confirmed {
        let confirmation: Confirmation = serde_json::from_slice(raw.payload()).unwrap();
        assert_eq!(confirmation.id, 7);
        assert!(confirmation.accepted);
    }

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[test]
fn batch_publishing_def_records_metadata() {
    let broker = MemoryBroker::new();
    let replies = TypedPublisher::new(broker.publisher());
    let app = RustStream::new(AppInfo::new("audit", "0.1.0"))
        .with_broker(broker, |b| b.include_batch_publishing(audit, replies));

    assert_eq!(app.handlers().len(), 1);
    assert_eq!(app.handlers()[0].name, "requests");
    assert!(
        app.handlers()[0]
            .output_type
            .is_some_and(|t| t.contains("Confirmation")),
    );
}

#[test]
fn batch_def_records_metadata() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(bill));

    assert_eq!(app.handlers().len(), 1);
    assert_eq!(app.handlers()[0].name, "orders");
    assert_eq!(
        app.handlers()[0].description.as_deref(),
        Some("Settles a whole page of orders at once."),
    );
}
