//! Integration tests for the `Router` include family (subscribe and batch forms), in both codec
//! forms: the default codec and a chain codec set with `with_codec`. Also covers `merge`, the
//! router's own `layer` stack, and `handlers()` metadata collection.
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
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream, layers::TracingLayer};
use ruststream::{Name, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

/// Publishes `payload` to each topic until every counter is non-zero (subscriptions open inside
/// `run()`, so early publishes are lost), bounded by a hang guard.
async fn drive_until_all_seen(
    publisher: &impl Publisher<Error = std::convert::Infallible>,
    topics: &[&str],
    counters: &[&AtomicUsize],
    signal: &Notify,
) {
    let payload = order_bytes(1);
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for topic in topics {
                let _ = publisher
                    .publish(OutgoingMessage::new(topic, &payload))
                    .await;
            }
            handler_signal(signal).await;
            if counters.iter().all(|c| c.load(Ordering::SeqCst) >= 1) {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "not every include form dispatched");
}

static RI_PLAIN: AtomicUsize = AtomicUsize::new(0);
static RI_ON: AtomicUsize = AtomicUsize::new(0);
static RI_BATCH: AtomicUsize = AtomicUsize::new(0);
static RI_BATCH_ON: AtomicUsize = AtomicUsize::new(0);
static RI_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("ri-plain")]
async fn ri_plain(_o: &Order) -> HandlerResult {
    RI_PLAIN.fetch_add(1, Ordering::SeqCst);
    RI_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("ri-ignored")]
async fn ri_on(_o: &Order) -> HandlerResult {
    RI_ON.fetch_add(1, Ordering::SeqCst);
    RI_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber(batch("ri-batch"))]
async fn ri_batch(orders: &[Order]) -> HandlerResult {
    RI_BATCH.fetch_add(orders.len(), Ordering::SeqCst);
    RI_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber(batch("ri-ignored"))]
async fn ri_batch_on(orders: &[Order]) -> HandlerResult {
    RI_BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    RI_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// All four default-codec router forms dispatch: `include` (macro source), `include_on`
/// (explicit source), `include_batch`, `include_batch_on`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_includes_dispatch() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include(ri_plain)
        .include_on(Name::new("ri-on"), ri_on)
        .include_batch(ri_batch)
        .include_batch_on(Name::new("ri-batch-on"), ri_batch_on);

    let app = RustStream::new(AppInfo::new("ri", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_all_seen(
        &publisher,
        &["ri-plain", "ri-on", "ri-batch", "ri-batch-on"],
        &[&RI_PLAIN, &RI_ON, &RI_BATCH, &RI_BATCH_ON],
        &RI_NOTIFY,
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static RC_PLAIN: AtomicUsize = AtomicUsize::new(0);
static RC_ON: AtomicUsize = AtomicUsize::new(0);
static RC_BATCH: AtomicUsize = AtomicUsize::new(0);
static RC_BATCH_ON: AtomicUsize = AtomicUsize::new(0);
static RC_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("rc-plain")]
async fn rc_plain(_o: &Order) -> HandlerResult {
    RC_PLAIN.fetch_add(1, Ordering::SeqCst);
    RC_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("rc-ignored")]
async fn rc_on(_o: &Order) -> HandlerResult {
    RC_ON.fetch_add(1, Ordering::SeqCst);
    RC_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber(batch("rc-batch"))]
async fn rc_batch(orders: &[Order]) -> HandlerResult {
    RC_BATCH.fetch_add(orders.len(), Ordering::SeqCst);
    RC_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber(batch("rc-ignored"))]
async fn rc_batch_on(orders: &[Order]) -> HandlerResult {
    RC_BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    RC_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// The same four forms decode through a chain codec named once with `with_codec`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_includes_dispatch() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(rc_plain)
        .include_on(Name::new("rc-on"), rc_on)
        .include_batch(rc_batch)
        .include_batch_on(Name::new("rc-batch-on"), rc_batch_on);

    let app = RustStream::new(AppInfo::new("rc", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_all_seen(
        &publisher,
        &["rc-plain", "rc-on", "rc-batch", "rc-batch-on"],
        &[&RC_PLAIN, &RC_ON, &RC_BATCH, &RC_BATCH_ON],
        &RC_NOTIFY,
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static RM_A: AtomicUsize = AtomicUsize::new(0);
static RM_B: AtomicUsize = AtomicUsize::new(0);
static RM_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("rm-a")]
async fn rm_a(_o: &Order) -> HandlerResult {
    RM_A.fetch_add(1, Ordering::SeqCst);
    RM_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("rm-b")]
async fn rm_b(_o: &Order) -> HandlerResult {
    RM_B.fetch_add(1, Ordering::SeqCst);
    RM_NOTIFY.notify_one();
    HandlerResult::Ack
}

/// `merge` keeps both routers' registrations (and their metadata order: own first, merged
/// after); a router-scope `layer` on the merged router still dispatches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_router_dispatches_and_collects_metadata() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let merged = Router::<MemoryBroker>::new().include(rm_a).merge(
        Router::<MemoryBroker>::new()
            .layer(TracingLayer::default())
            .include(rm_b),
    );

    let names: Vec<_> = merged.handlers().into_iter().map(|m| m.name).collect();
    assert_eq!(names, ["rm-a", "rm-b"]);

    let app = RustStream::new(AppInfo::new("rm", "0.1.0"))
        .with_broker(broker, |b| b.include_router(merged));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_all_seen(&publisher, &["rm-a", "rm-b"], &[&RM_A, &RM_B], &RM_NOTIFY).await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
