//! Integration tests for the unified failure policy: handler panics and decode failures, settled
//! by the per-subscriber `on_failure(panic = .., decode = ..)` policy. Driven over `MemoryBroker`.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Order, Wire};

use ruststream::memory::prelude::*;
use ruststream::runtime::RustStreamError;
use ruststream::testing::{Outcome, TestApp};

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
    order.id
}

/// A plain batch handler: exercises the per-element batch decode-failure path.
#[subscriber("bd")]
async fn bd(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

/// A batch publishing handler: exercises the batch-publishing decode-failure path.
#[subscriber("bpd", publish("bpd.out"))]
async fn bpd(orders: &[Order]) -> Vec<u32> {
    orders.iter().map(|o| o.id).collect()
}

/// Injects a good order, a payload the decoder rejects, and another good order, in that order.
///
/// Every migrated policy test drives the same three deliveries: what differs is how the middle
/// one is settled and whether the service is still there for the third.
async fn drive_good_bad_good<S: Send + Sync + 'static>(tb: &TestApp<S>, topic: &str) {
    tb.message(&Order { id: 1 })
        .to(topic)
        .publish()
        .await
        .expect("publish");
    tb.message(&Wire::of(b"not json"))
        .to(topic)
        .publish()
        .await
        .expect("publish");
    tb.message(&Order { id: 2 })
        .to(topic)
        .publish()
        .await
        .expect("publish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_panic_fails_fast_and_run_returns_err() {
    let app =
        RustStream::new(AppInfo::new("boom", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(boom);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("boom")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("boom")
        .assert_called_once()
        .panicked();
    tb.assert_shut_down();
    let result = tb.shutdown().await;
    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "a fail-fast panic must make run() return a dispatch error, got {result:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_drop_keeps_the_subscriber_consuming() {
    let app =
        RustStream::new(AppInfo::new("dropping", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(dropping);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    // A good order, then the poison order that panics (dropped), then a good order that must
    // still be processed.
    for id in [7u32, 0, 9] {
        tb.message(&Order { id })
            .to("dropping")
            .publish()
            .await
            .expect("publish");
    }

    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("dropping")
            .outcomes(),
        [Outcome::Ack, Outcome::Panicked, Outcome::Ack],
    );
    tb.assert_running();
    let result = tb.shutdown().await;
    assert!(
        result.is_ok(),
        "a dropped panic must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_fail_fast_returns_err() {
    let app =
        RustStream::new(AppInfo::new("decodeff", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(decode_ff);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Wire::of(b"not json"))
        .to("decodeff")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("decodeff")
        .assert_called_once()
        .assert_last_failed_to_decode();
    tb.assert_shut_down();
    let result = tb.shutdown().await;
    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "a fail-fast decode failure must make run() return a dispatch error, got {result:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_skip_acks_past_bad_input_and_continues() {
    let app =
        RustStream::new(AppInfo::new("skipping", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(skipping);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_good_bad_good(&tb, "skipping").await;

    // The malformed payload is acked past rather than requeued, and the good order behind it is
    // still processed.
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("skipping")
            .outcomes(),
        [Outcome::Ack, Outcome::DecodeFailed, Outcome::Ack],
    );
    tb.assert_running();
    let result = tb.shutdown().await;
    assert!(
        result.is_ok(),
        "a skipped decode failure must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_decode_failure_is_dropped_and_continues() {
    let router = Router::<MemoryBroker>::new()
        .include(rpcd)
        .out(Reply, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rpcd", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_good_bad_good(&tb, "rpcd").await;

    assert_eq!(
        tb.broker::<MemoryBroker>().subscriber("rpcd").outcomes(),
        [Outcome::Ack, Outcome::DecodeFailed, Outcome::Ack],
    );
    // The element that never decoded published no reply; the two that did, did.
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .published::<u32>("rpcd.out")
            .decoded(),
        vec![1, 2],
    );
    tb.assert_running();
    let result = tb.shutdown().await;
    assert!(
        result.is_ok(),
        "a dropped decode failure must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_decode_failure_drops_the_bad_element() {
    let app = RustStream::new(AppInfo::new("bd", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(bd.batch(nonzero!(64)));
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_good_bad_good(&tb, "bd").await;

    // The rejected element is settled by the policy and never becomes part of a page, so the
    // handler saw exactly the two decodable orders.
    let seen: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("bd").received();
    assert_eq!(seen.iter().map(|o| o.id).collect::<Vec<_>>(), [1, 2]);
    tb.broker::<MemoryBroker>()
        .subscriber("bd")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.assert_running();
    let result = tb.shutdown().await;
    assert!(
        result.is_ok(),
        "a dropped batch element must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_publishing_decode_failure_is_dropped() {
    let router = Router::<MemoryBroker>::new()
        .include(bpd.batch(nonzero!(64)))
        .out(Reply, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("bpd", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_good_bad_good(&tb, "bpd").await;

    let seen: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("bpd").received();
    assert_eq!(seen.iter().map(|o| o.id).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .published::<u32>("bpd.out")
            .decoded(),
        vec![1, 2],
    );
    tb.assert_running();
    let result = tb.shutdown().await;
    assert!(
        result.is_ok(),
        "a dropped batch reply element must not error the run: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_handler_panic_fails_fast() {
    let app =
        RustStream::new(AppInfo::new("batchboom", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(batch_boom.batch(nonzero!(64)));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("batchboom")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("batchboom")
        .assert_called_once()
        .panicked();
    tb.assert_shut_down();
    let result = tb.shutdown().await;
    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "a fail-fast batch panic must make run() return a dispatch error, got {result:?}",
    );
}
