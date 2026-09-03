//! Integration tests pinning the composition rules documented in the Subscribers guide: the
//! feature pairs (transactional x workers, Buffered x workers, publishing x workers) whose
//! interaction is promised in prose. The remaining pairs are pinned elsewhere: workers x batch
//! and retry x pools / lanes in `workers.rs` and `retry_semantics.rs`.
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so every
//! message is published exactly once - no republish loops.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use common::{Order, Receipt, connected, wait_for};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream, TypedPublisher};
use ruststream::testing::expect_published;
use ruststream::{Buffered, Name, nonzero, subscriber};

static TX_HANDLED: AtomicUsize = AtomicUsize::new(0);

/// Each batch's replies go out in one transaction; the pool runs the batches concurrently.
#[subscriber("tx-in", publish("tx-out"), workers(2))]
async fn tx_confirm(orders: &[Order]) -> Vec<Receipt> {
    TX_HANDLED.fetch_add(orders.len(), Ordering::SeqCst);
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

/// Transactional reply publishing composes with a batch pool: every delivered order is
/// confirmed exactly through its own batch's transaction, with batches in flight concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactional_replies_compose_with_a_batch_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    // The observing side needs the TestableBroker surface, which lives on the connected form;
    // the shared in-process bus makes this clone observe the app's broker.
    let observer = connected(&broker).await;

    let replies = TypedPublisher::new(MemoryPublish).transactional();
    let app = RustStream::new(AppInfo::new("tx", "0.1.0")).with_broker(broker, |b| {
        b.include(tx_confirm).publisher(replies);
    });

    let running = app.start().await.expect("startup failed");

    // Four orders, each published once; expect one committed receipt per handled order.
    for id in 1..=4u32 {
        publisher
            .message(&Order { id })
            .to("tx-in")
            .publish()
            .await
            .expect("publish");
    }
    wait_for(
        || TX_HANDLED.load(Ordering::SeqCst) >= 4,
        Duration::from_secs(5),
    )
    .await;

    let handled = TX_HANDLED.load(Ordering::SeqCst);
    let receipts = expect_published(&observer, "tx-out", handled, Duration::from_secs(5)).await;
    assert!(
        receipts.len() >= handled,
        "{} orders handled but only {} receipts committed",
        handled,
        receipts.len(),
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

static BUF_SEEN: AtomicUsize = AtomicUsize::new(0);
static BUF_BATCHES: AtomicUsize = AtomicUsize::new(0);

/// Client-side batching under a pool: the size cap or deadline (not the pool) closes a batch.
#[subscriber(Buffered::<Name>::new(Name::new("buf-in"))
    .max_size(nonzero!(2))
    .max_wait(Duration::from_millis(10)), workers(2))]
async fn buffered_drain(orders: &[Order]) -> HandlerOutcome {
    BUF_SEEN.fetch_add(orders.len(), Ordering::SeqCst);
    BUF_BATCHES.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// The Buffered adapter composes with a batch pool: batches still close by size or deadline,
/// the pool only bounds how many are processed at once. Every delivery is drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_sources_compose_with_a_batch_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("buf", "0.1.0"))
        .with_broker(broker, |b| b.include(buffered_drain));

    let running = app.start().await.expect("startup failed");

    // Six deliveries against a size cap of two: they cannot all fit in one batch.
    for id in 1..=6u32 {
        publisher
            .message(&Order { id })
            .to("buf-in")
            .publish()
            .await
            .expect("publish");
    }
    wait_for(
        || BUF_SEEN.load(Ordering::SeqCst) >= 6,
        Duration::from_secs(5),
    )
    .await;
    assert!(
        BUF_BATCHES.load(Ordering::SeqCst) >= 2,
        "everything arrived as a single batch; size/deadline closing did not engage",
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

static PUB_REPLIED: AtomicUsize = AtomicUsize::new(0);

/// Reply publishing under a pool: replies are produced concurrently.
#[subscriber("pub-in", publish("pub-out"), workers(3))]
async fn pooled_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("pub-out")]
async fn pooled_check(_r: &Receipt) -> HandlerOutcome {
    PUB_REPLIED.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// A publishing handler composes with a worker pool: every delivery's reply arrives; reply
/// order across deliveries is not promised (the pool processes them concurrently).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_replies_compose_with_a_worker_pool() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pub", "0.1.0")).with_broker(broker, |b| {
        b.include(pooled_relay);
        b.include(pooled_check);
    });

    let running = app.start().await.expect("startup failed");

    for id in 1..=4u32 {
        publisher
            .message(&Order { id })
            .to("pub-in")
            .publish()
            .await
            .expect("publish");
    }
    wait_for(
        || PUB_REPLIED.load(Ordering::SeqCst) >= 4,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}
