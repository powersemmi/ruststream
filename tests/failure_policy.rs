//! Integration tests for the unified failure policy: handler panics and decode failures, settled
//! by the per-subscriber `on_failure(panic = .., decode = ..)` policy. Driven over `MemoryBroker`.
#![cfg(feature = "macros")]

mod common;

use common::{Order, Wire, order_bytes, wait_for};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, HandlerOutcome, PublishExt, Router, RustStream, RustStreamError, TypedPublisher,
};
use ruststream::{Publisher, subscriber};

// Counters keyed per handler so the parallel tests do not interfere; each handler is used by one
// test only.
static DROP_DONE: AtomicUsize = AtomicUsize::new(0);
static SKIP_DONE: AtomicUsize = AtomicUsize::new(0);
static RPC_DONE: AtomicUsize = AtomicUsize::new(0);
static BATCH_DONE: AtomicUsize = AtomicUsize::new(0);
static BATCH_REPLY_DONE: AtomicUsize = AtomicUsize::new(0);

/// Default policy: a panic fails fast. Used by `handler_panic_fails_fast_and_run_returns_err`.
#[subscriber("boom")]
async fn boom(order: &Order) -> HandlerOutcome {
    // The test publishes ids other than u32::MAX, so this assertion always fails (panics); the
    // trailing expression keeps the body typed as HandlerOutcome.
    assert_eq!(order.id, u32::MAX, "handler exploded");
    HandlerOutcome::ack()
}

/// `panic = drop` settles the offending message and keeps consuming. The poison id is 0.
#[subscriber("dropping", on_failure(panic = drop))]
async fn dropping(order: &Order) -> HandlerOutcome {
    assert!(order.id != 0, "poison order must panic");
    DROP_DONE.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// `decode = fail_fast` tears the service down on a payload that cannot decode.
#[subscriber("decodeff", on_failure(decode = fail_fast))]
async fn decode_ff(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// `decode = skip` acks past a payload that cannot decode and keeps consuming.
#[subscriber("skipping", on_failure(decode = skip))]
async fn skipping(_order: &Order) -> HandlerOutcome {
    SKIP_DONE.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// A batch handler under an explicit `panic = fail_fast`.
#[subscriber("batchboom", on_failure(panic = fail_fast))]
async fn batch_boom(orders: &[Order]) -> HandlerOutcome {
    // The test always delivers a non-empty batch, so this assertion always fails (panics).
    assert!(orders.is_empty(), "batch handler exploded");
    HandlerOutcome::ack()
}

/// A publishing handler: exercises the single-message decode-failure path (default `decode = drop`).
#[subscriber("rpcd", publish("rpcd.out"))]
async fn rpcd(order: &Order) -> u32 {
    RPC_DONE.fetch_add(1, Ordering::SeqCst);
    order.id
}

/// A plain batch handler: exercises the per-element batch decode-failure path.
#[subscriber("bd")]
async fn bd(orders: &[Order]) -> HandlerOutcome {
    BATCH_DONE.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// A batch publishing handler: exercises the batch-publishing decode-failure path.
#[subscriber("bpd", publish("bpd.out"))]
async fn bpd(orders: &[Order]) -> Vec<u32> {
    BATCH_REPLY_DONE.fetch_add(orders.len(), Ordering::SeqCst);
    orders.iter().map(|o| o.id).collect()
}

/// Spawns `run_until(pending)` and publishes `poison` to `topic` until the service tears itself
/// down, then returns the run result.
///
/// The poison travels as a wire: what tears the service down differs per caller - a payload the
/// handler panics on, or one the decoder rejects - so the shape they share is the bytes.
async fn run_until_torn_down(
    app: RustStream,
    publisher: impl Publisher,
    topic: &str,
    poison: &Wire,
) -> Result<(), RustStreamError> {
    let running = app.start().await.expect("startup failed");
    // `start` resolves with the subscription open, so one poison message is enough.
    let _ = publisher.message(poison).to(topic).publish().await;
    tokio::time::timeout(Duration::from_secs(5), running.stopping())
        .await
        .expect("service did not tear down within the deadline");
    running.shutdown().await
}

/// Publishes `order` to `topic` once (`start` resolves with subscriptions open) and waits until
/// `counter` advances, proving the handler ran.
async fn drive_until_seen(
    publisher: &impl Publisher,
    topic: &str,
    order: &Order,
    counter: &AtomicUsize,
) {
    let start = counter.load(Ordering::SeqCst);
    let _ = publisher.message(order).to(topic).publish().await;
    wait_for(
        || counter.load(Ordering::SeqCst) > start,
        Duration::from_secs(5),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_panic_fails_fast_and_run_returns_err() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("boom", "0.1.0")).with_broker(broker, |b| {
        b.include(boom);
    });

    let result = run_until_torn_down(app, publisher, "boom", &Wire::of(order_bytes(1))).await;
    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "a fail-fast panic must make run() return a dispatch error, got {result:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_drop_keeps_the_subscriber_consuming() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("dropping", "0.1.0")).with_broker(broker, |b| {
        b.include(dropping);
    });

    let running = app.start().await.expect("startup failed");

    drive_until_seen(&publisher, "dropping", &Order { id: 7 }, &DROP_DONE).await;
    let before = DROP_DONE.load(Ordering::SeqCst);

    // A poison order panics (dropped), then a good order must still be processed.
    publisher
        .message(&Order { id: 0 })
        .to("dropping")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Order { id: 9 })
        .to("dropping")
        .publish()
        .await
        .unwrap();

    wait_for(
        || DROP_DONE.load(Ordering::SeqCst) > before,
        Duration::from_secs(5),
    )
    .await;

    let result = tokio::time::timeout(Duration::from_secs(5), running.shutdown())
        .await
        .expect("shutdown did not finish");
    assert!(
        result.is_ok(),
        "a dropped panic must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_fail_fast_returns_err() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("decodeff", "0.1.0")).with_broker(broker, |b| {
        b.include(decode_ff);
    });

    let result = run_until_torn_down(app, publisher, "decodeff", &Wire::of(b"not json")).await;
    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "a fail-fast decode failure must make run() return a dispatch error, got {result:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_skip_acks_past_bad_input_and_continues() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("skipping", "0.1.0")).with_broker(broker, |b| {
        b.include(skipping);
    });

    let running = app.start().await.expect("startup failed");

    drive_until_seen(&publisher, "skipping", &Order { id: 1 }, &SKIP_DONE).await;
    let before = SKIP_DONE.load(Ordering::SeqCst);

    // A malformed payload is skipped (acked past), then a good order is still processed.
    publisher
        .message(&Wire::of(b"not json"))
        .to("skipping")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Order { id: 2 })
        .to("skipping")
        .publish()
        .await
        .unwrap();

    wait_for(
        || SKIP_DONE.load(Ordering::SeqCst) > before,
        Duration::from_secs(5),
    )
    .await;

    let result = tokio::time::timeout(Duration::from_secs(5), running.shutdown())
        .await
        .expect("shutdown did not finish");
    assert!(
        result.is_ok(),
        "a skipped decode failure must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_decode_failure_is_dropped_and_continues() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let router = Router::<MemoryBroker>::new()
        .include(rpcd)
        .publisher(TypedPublisher::new(MemoryPublish));
    let app = RustStream::new(AppInfo::new("rpcd", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    drive_until_seen(&publisher, "rpcd", &Order { id: 1 }, &RPC_DONE).await;
    let before = RPC_DONE.load(Ordering::SeqCst);
    publisher
        .message(&Wire::of(b"not json"))
        .to("rpcd")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Order { id: 2 })
        .to("rpcd")
        .publish()
        .await
        .unwrap();
    wait_for(
        || RPC_DONE.load(Ordering::SeqCst) > before,
        Duration::from_secs(5),
    )
    .await;

    let result = tokio::time::timeout(Duration::from_secs(5), running.shutdown())
        .await
        .expect("shutdown did not finish");
    assert!(
        result.is_ok(),
        "a dropped decode failure must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_decode_failure_drops_the_bad_element() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("bd", "0.1.0")).with_broker(broker, |b| {
        b.include(bd);
    });

    let running = app.start().await.expect("startup failed");

    drive_until_seen(&publisher, "bd", &Order { id: 1 }, &BATCH_DONE).await;
    let before = BATCH_DONE.load(Ordering::SeqCst);
    publisher
        .message(&Wire::of(b"not json"))
        .to("bd")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Order { id: 2 })
        .to("bd")
        .publish()
        .await
        .unwrap();
    wait_for(
        || BATCH_DONE.load(Ordering::SeqCst) > before,
        Duration::from_secs(5),
    )
    .await;

    let result = tokio::time::timeout(Duration::from_secs(5), running.shutdown())
        .await
        .expect("shutdown did not finish");
    assert!(
        result.is_ok(),
        "a dropped batch element must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_publishing_decode_failure_is_dropped() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let router = Router::<MemoryBroker>::new()
        .include(bpd)
        .publisher(TypedPublisher::new(MemoryPublish));
    let app = RustStream::new(AppInfo::new("bpd", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    drive_until_seen(&publisher, "bpd", &Order { id: 1 }, &BATCH_REPLY_DONE).await;
    let before = BATCH_REPLY_DONE.load(Ordering::SeqCst);
    publisher
        .message(&Wire::of(b"not json"))
        .to("bpd")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Order { id: 2 })
        .to("bpd")
        .publish()
        .await
        .unwrap();
    wait_for(
        || BATCH_REPLY_DONE.load(Ordering::SeqCst) > before,
        Duration::from_secs(5),
    )
    .await;

    let result = tokio::time::timeout(Duration::from_secs(5), running.shutdown())
        .await
        .expect("shutdown did not finish");
    assert!(
        result.is_ok(),
        "a dropped batch reply element must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_handler_panic_fails_fast() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("batchboom", "0.1.0")).with_broker(broker, |b| {
        b.include(batch_boom);
    });

    let result = run_until_torn_down(app, publisher, "batchboom", &Wire::of(order_bytes(1))).await;
    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "a fail-fast batch panic must make run() return a dispatch error, got {result:?}",
    );
}
