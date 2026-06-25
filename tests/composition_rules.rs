//! Integration tests pinning the composition rules documented in the Subscribers guide: the
//! feature pairs (transactional x workers, Buffered x workers, publishing x workers) whose
//! interaction is promised in prose. The remaining pairs are pinned elsewhere: workers x batch
//! and retry x pools / lanes in `workers.rs` and `retry_semantics.rs`.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::handler_signal;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::testing::expect_published;
use ruststream::{Buffered, Name, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Receipt {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

static TX_HANDLED: AtomicUsize = AtomicUsize::new(0);
static TX_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Each batch's replies go out in one transaction; the pool runs the batches concurrently.
#[subscriber(batch("tx-in"), publish("tx-out"), workers(2))]
async fn tx_confirm(orders: &[Order]) -> Vec<Receipt> {
    TX_HANDLED.fetch_add(orders.len(), Ordering::SeqCst);
    TX_NOTIFY.notify_one();
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

/// Transactional reply publishing composes with a batch pool: every delivered order is
/// confirmed exactly through its own batch's transaction, with batches in flight concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactional_replies_compose_with_a_batch_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let observer = broker.clone();

    let replies = TypedPublisher::new(broker.publisher()).transactional();
    let app = RustStream::new(AppInfo::new("tx", "0.1.0"))
        .with_broker(broker, |b| b.include_batch_publishing(tx_confirm, replies));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Publish until at least four orders made it through (early publishes are lost while the
    // subscription opens), then expect one committed receipt per handled order.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut id = 1u32;
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("tx-in", &order_bytes(id)))
                .await;
            id += 1;
            handler_signal(&TX_NOTIFY).await;
            if TX_HANDLED.load(Ordering::SeqCst) >= 4 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "batches did not flow through the pool");

    let handled = TX_HANDLED.load(Ordering::SeqCst);
    let receipts = expect_published(&observer, "tx-out", handled, Duration::from_secs(5)).await;
    assert!(
        receipts.len() >= handled,
        "{} orders handled but only {} receipts committed",
        handled,
        receipts.len(),
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static BUF_SEEN: AtomicUsize = AtomicUsize::new(0);
static BUF_BATCHES: AtomicUsize = AtomicUsize::new(0);
static BUF_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Client-side batching under a pool: the deadline (not the pool) closes a batch.
#[subscriber(batch(Buffered::<Name>::new(Name::new("buf-in"))
    .max_size(2)
    .max_wait(Duration::from_millis(10))), workers(2))]
async fn buffered_drain(orders: &[Order]) -> HandlerResult {
    BUF_SEEN.fetch_add(orders.len(), Ordering::SeqCst);
    BUF_BATCHES.fetch_add(1, Ordering::SeqCst);
    BUF_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// The Buffered adapter composes with a batch pool: batches still close by size or deadline,
/// the pool only bounds how many are processed at once. Every delivery is drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_sources_compose_with_a_batch_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("buf", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(buffered_drain));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut id = 1u32;
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("buf-in", &order_bytes(id)))
                .await;
            id += 1;
            handler_signal(&BUF_NOTIFY).await;
            if BUF_SEEN.load(Ordering::SeqCst) >= 6 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "buffered batches did not drain through the pool"
    );
    assert!(
        BUF_BATCHES.load(Ordering::SeqCst) >= 2,
        "everything arrived as a single batch; size/deadline closing did not engage",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static PUB_REPLIED: AtomicUsize = AtomicUsize::new(0);
static PUB_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Reply publishing under a pool: replies are produced concurrently.
#[subscriber("pub-in", publish("pub-out"), workers(3))]
async fn pooled_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("pub-out")]
async fn pooled_check(_r: &Receipt) -> HandlerResult {
    PUB_REPLIED.fetch_add(1, Ordering::SeqCst);
    PUB_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// A publishing handler composes with a worker pool: every delivery's reply arrives; reply
/// order across deliveries is not promised (the pool processes them concurrently).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_replies_compose_with_a_worker_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pub", "0.1.0")).with_broker(broker, |b| {
        b.include_publishing(pooled_relay, TypedPublisher::new(b.broker().publisher()));
        b.include(pooled_check);
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut id = 1u32;
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pub-in", &order_bytes(id)))
                .await;
            id += 1;
            handler_signal(&PUB_NOTIFY).await;
            if PUB_REPLIED.load(Ordering::SeqCst) >= 4 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "pooled publishing handler did not reply to every delivery",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
