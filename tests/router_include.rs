//! Integration tests for the `Router` include family (subscribe and batch forms), in both codec
//! forms: the default codec and a chain codec set with `with_codec`. Also covers `merge`, the
//! router's own `layer` stack, and `handlers()` metadata collection.
#![cfg(feature = "macros")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Order, order_bytes, wait_for};
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::runtime::{
    AppInfo, HandlerResult, PublishExt, Router, RustStream, layers::TracingLayer,
};
use ruststream::{Publisher, subscriber};

/// Publishes `payload` once to each topic (the app is already started, so the subscriptions are
/// open and every publish lands), then waits until every counter is non-zero.
async fn publish_and_await_all(
    publisher: &impl Publisher,
    topics: &[&str],
    counters: &[&AtomicUsize],
) {
    let payload = order_bytes(1);
    for topic in topics {
        publisher
            .raw(&payload)
            .to(*topic)
            .publish()
            .await
            .expect("publish");
    }
    wait_for(
        || counters.iter().all(|c| c.load(Ordering::SeqCst) >= 1),
        Duration::from_secs(5),
    )
    .await;
}

static RI_PLAIN: AtomicUsize = AtomicUsize::new(0);
static RI_ON: AtomicUsize = AtomicUsize::new(0);
static RI_BATCH: AtomicUsize = AtomicUsize::new(0);
static RI_BATCH_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber("ri-plain")]
async fn ri_plain(_o: &Order) -> HandlerResult {
    RI_PLAIN.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

// A broker source expression rather than a bare name: the definition is where a subscription
// source belongs, and the attribute takes the broker's own source builder.
#[subscriber(MemorySource::new("ri-on"))]
async fn ri_on(_o: &Order) -> HandlerResult {
    RI_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch("ri-batch"))]
async fn ri_batch(orders: &[Order]) -> HandlerResult {
    RI_BATCH.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch(MemorySource::new("ri-batch-on")))]
async fn ri_batch_on(orders: &[Order]) -> HandlerResult {
    RI_BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

/// The default-codec router forms dispatch, whether the definition names its source as a topic
/// string or builds one with the broker's own source type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_includes_dispatch() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include(ri_plain)
        .include(ri_on)
        .include(ri_batch)
        .include(ri_batch_on);

    let app = RustStream::new(AppInfo::new("ri", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    publish_and_await_all(
        &publisher,
        &["ri-plain", "ri-on", "ri-batch", "ri-batch-on"],
        &[&RI_PLAIN, &RI_ON, &RI_BATCH, &RI_BATCH_ON],
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static RC_PLAIN: AtomicUsize = AtomicUsize::new(0);
static RC_ON: AtomicUsize = AtomicUsize::new(0);
static RC_BATCH: AtomicUsize = AtomicUsize::new(0);
static RC_BATCH_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber("rc-plain")]
async fn rc_plain(_o: &Order) -> HandlerResult {
    RC_PLAIN.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(MemorySource::new("rc-on"))]
async fn rc_on(_o: &Order) -> HandlerResult {
    RC_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch("rc-batch"))]
async fn rc_batch(orders: &[Order]) -> HandlerResult {
    RC_BATCH.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch(MemorySource::new("rc-batch-on")))]
async fn rc_batch_on(orders: &[Order]) -> HandlerResult {
    RC_BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

/// The same four registrations decode through a chain codec named once with `with_codec`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_includes_dispatch() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(rc_plain)
        .include(rc_on)
        .include(rc_batch)
        .include(rc_batch_on);

    let app = RustStream::new(AppInfo::new("rc", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    publish_and_await_all(
        &publisher,
        &["rc-plain", "rc-on", "rc-batch", "rc-batch-on"],
        &[&RC_PLAIN, &RC_ON, &RC_BATCH, &RC_BATCH_ON],
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static RM_A: AtomicUsize = AtomicUsize::new(0);
static RM_B: AtomicUsize = AtomicUsize::new(0);

#[subscriber("rm-a")]
async fn rm_a(_o: &Order) -> HandlerResult {
    RM_A.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("rm-b")]
async fn rm_b(_o: &Order) -> HandlerResult {
    RM_B.fetch_add(1, Ordering::SeqCst);
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

    let running = app.start().await.expect("startup failed");

    publish_and_await_all(&publisher, &["rm-a", "rm-b"], &[&RM_A, &RM_B]).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
