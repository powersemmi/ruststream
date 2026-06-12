//! Integration tests for the batch subscriber pipeline: the `#[subscriber(batch(..))]` form,
//! `include_batch` mounting, per-element decode failures, and the `Buffered` adapter.
#![cfg(feature = "macros")]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream};
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

/// Settles a whole page of orders at once.
#[subscriber(batch("orders"))]
async fn bill(orders: &[Order]) -> HandlerResult {
    BATCHES
        .lock()
        .unwrap()
        .push(orders.iter().map(|o| o.id).collect());
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

    // The subscription opens inside run(); retry until deliveries land.
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            for id in 0..3u32 {
                let _ = publisher
                    .publish(OutgoingMessage::new("orders", &order_bytes(id)))
                    .await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
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

/// Records the ids that survived decoding.
#[subscriber(batch("mixed"))]
async fn sift(orders: &[Order]) -> HandlerResult {
    GOOD_IDS.lock().unwrap().extend(orders.iter().map(|o| o.id));
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

    let result = tokio::time::timeout(Duration::from_secs(1), async {
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
            tokio::time::sleep(Duration::from_millis(10)).await;
            if GOOD_IDS.lock().unwrap().len() >= 2 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "decodable elements did not arrive");

    // Only decodable elements reach the handler; the garbage one is dropped individually.
    let ids = GOOD_IDS.lock().unwrap().clone();
    assert!(ids.starts_with(&[1, 2]), "got {ids:?}");

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static BUFFERED_SEEN: AtomicUsize = AtomicUsize::new(0);

/// A handler mounted on a `Buffered`-wrapped source directly in the macro. The macro recovers
/// the source type from the constructor path, so a generic source spells its parameter
/// (turbofish).
#[subscriber(batch(Buffered::<Name>::new(Name::new("events")).max_size(2)))]
async fn drain(events: &[Order]) -> HandlerResult {
    BUFFERED_SEEN.fetch_add(events.len(), Ordering::SeqCst);
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

    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("events", &order_bytes(7)))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
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
static RETRIED_ONCE: AtomicBool = AtomicBool::new(false);

/// Retries order 11 on first sight; settles everything else, per element.
#[subscriber(batch("pages"))]
async fn reconcile(orders: &[Order]) -> Vec<HandlerResult> {
    orders
        .iter()
        .map(|o| {
            if o.id == 11 && !RETRIED_ONCE.swap(true, Ordering::SeqCst) {
                HandlerResult::retry()
            } else {
                SETTLED.lock().unwrap().push(o.id);
                HandlerResult::Ack
            }
        })
        .collect()
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
    let warmup = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pages", &order_bytes(0)))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
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

    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
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
