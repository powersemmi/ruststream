//! Integration tests pinning the composition rules documented in the Subscribers guide: the
//! feature pairs (transactional x workers, Buffered x workers, publishing x workers) whose
//! interaction is promised in prose. The remaining pairs are pinned elsewhere: workers x batch
//! and retry x pools / lanes in `workers.rs` and `retry_semantics.rs`.
//!
//! Each suite injects its deliveries together: a pool and a size-capped buffer only compose with
//! anything while several deliveries are in flight, and the harness settles the whole reaction
//! before the injections resolve.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::time::Duration;

use common::{Order, Receipt};
use futures::future::join_all;
use ruststream::Buffered;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

/// Each batch's replies go out in one transaction; the pool runs the batches concurrently.
#[subscriber("tx-in", publish("tx-out"), workers(2))]
async fn tx_confirm(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

/// The ids of every order the injections below carry.
fn orders(count: u32) -> Vec<Order> {
    (1..=count).map(|id| Order { id }).collect()
}

/// Sorted ids, so an assertion can name what arrived without naming the order a pool chose.
fn sorted_ids(receipts: &[Receipt]) -> Vec<u32> {
    let mut ids: Vec<u32> = receipts.iter().map(|r| r.id).collect();
    ids.sort_unstable();
    ids
}

/// Transactional reply publishing composes with a batch pool: every delivered order is
/// confirmed exactly through its own batch's transaction, with batches in flight concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactional_replies_compose_with_a_batch_pool() {
    let app = RustStream::new(AppInfo::new("tx", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(tx_confirm.batch(nonzero!(4)))
            .out(Reply, TransactionalPublish)
            .transactional();
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    // Four orders, each published once; expect one committed receipt per handled order.
    let input = orders(4);
    for result in join_all(
        input
            .iter()
            .map(|order| tb.message(order).to("tx-in").publish()),
    )
    .await
    {
        result.expect("publish");
    }

    let receipts: Vec<Receipt> = tb
        .broker::<MemoryBroker>()
        .published::<Receipt>("tx-out")
        .decoded();
    assert_eq!(
        sorted_ids(&receipts),
        [1, 2, 3, 4],
        "every handled order must be confirmed exactly once",
    );
}

/// Client-side paging under a pool: the page size or the adapter's deadline (not the pool)
/// closes a page. The adapter is broker-author machinery, spelled here by hand to pin the
/// composition; the size stays the mount site's.
#[subscriber(Buffered::<Name>::new(Name::new("buf-in"))
    .max_wait(Duration::from_millis(10)), workers(2))]
async fn buffered_drain(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

/// The Buffered adapter composes with a batch pool: pages still close by size or deadline,
/// the pool only bounds how many are processed at once. Every delivery is drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_sources_compose_with_a_batch_pool() {
    let app = RustStream::new(AppInfo::new("buf", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(buffered_drain.batch(nonzero!(2)));
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    // Six deliveries against a size cap of two: they cannot all fit in one batch.
    let input = orders(6);
    for result in join_all(
        input
            .iter()
            .map(|order| tb.message(order).to("buf-in").publish()),
    )
    .await
    {
        result.expect("publish");
    }

    let pages: Vec<Vec<Order>> = tb.broker::<MemoryBroker>().subscriber("buf-in").pages();
    let mut drained: Vec<u32> = pages.iter().flatten().map(|o| o.id).collect();
    // The pool runs pages concurrently, so which page lands first is its business; that every
    // delivery ends up in one is not.
    drained.sort_unstable();
    assert_eq!(
        drained,
        [1, 2, 3, 4, 5, 6],
        "every delivery must be drained"
    );
    assert!(
        pages.iter().all(|page| page.len() <= 2),
        "the size cap must close a batch before the pool does: {:?}",
        pages.iter().map(Vec::len).collect::<Vec<_>>(),
    );
}

/// Reply publishing under a pool: replies are produced concurrently.
#[subscriber("pub-in", publish("pub-out"), workers(3))]
async fn pooled_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("pub-out")]
async fn pooled_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// A publishing handler composes with a worker pool: every delivery's reply arrives; reply
/// order across deliveries is not promised (the pool processes them concurrently).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_replies_compose_with_a_worker_pool() {
    let app = RustStream::new(AppInfo::new("pub", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(pooled_relay);
        b.include(pooled_check);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    let input = orders(4);
    for result in join_all(
        input
            .iter()
            .map(|order| tb.message(order).to("pub-in").publish()),
    )
    .await
    {
        result.expect("publish");
    }

    let replied: Vec<Receipt> = tb.broker::<MemoryBroker>().subscriber("pub-out").received();
    assert_eq!(
        sorted_ids(&replied),
        [1, 2, 3, 4],
        "every delivery's reply must arrive",
    );
}
