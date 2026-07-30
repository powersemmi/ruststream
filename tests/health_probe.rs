//! Integration tests for the [`RunningApp`] health probe: the terminal state survives the
//! consumed app handle, and a fail-fast teardown flips the probe with no `shutdown` call - the
//! process may keep running a sibling HTTP task, which is exactly when the probe matters.
//!
//! [`RunningApp`]: ruststream::runtime::RunningApp
#![cfg(feature = "macros")]

use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, HealthState, RustStream, RustStreamError};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

#[subscriber("health.ok")]
async fn fine(_order: &Order) -> HandlerResult {
    HandlerResult::Ack
}

/// Default policy: a panic fails fast, tearing the started service down.
#[subscriber("health.boom")]
async fn explode(order: &Order) -> HandlerResult {
    // The test publishes ids other than u32::MAX, so this assertion always fails (panics); the
    // trailing expression keeps the body typed as HandlerResult.
    assert_eq!(order.id, u32::MAX, "handler exploded");
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_reports_stopped() {
    let broker = MemoryBroker::new();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(fine));

    let running = app.start().await.expect("startup failed");
    let health = running.health();
    assert!(health.is_running(), "a started service must report Running");

    running.shutdown().await.expect("graceful shutdown failed");
    assert_eq!(
        health.state(),
        HealthState::Stopped,
        "the probe outlives the consumed app handle and reports the terminal state",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_fast_flips_the_probe_without_a_shutdown_call() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(explode));

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();

    publisher
        .publish(OutgoingMessage::new("health.boom", &order_bytes(1)))
        .await
        .expect("publish failed");

    // Nobody calls shutdown here: the fail-fast watcher alone must flip the probe, because in
    // the real deployment the HTTP task is still serving /healthz off it.
    let state = tokio::time::timeout(Duration::from_secs(5), health.changed())
        .await
        .expect("the probe never observed the fail-fast teardown");
    match state {
        HealthState::Failed { reason } => assert!(
            reason.contains("health.boom"),
            "the reason must name the failing subscription, got: {reason}",
        ),
        other => panic!("expected Failed, got {other:?}"),
    }

    // The orderly teardown afterwards still surfaces the dispatch failure to the caller.
    let err = running
        .shutdown()
        .await
        .expect_err("fail-fast must surface");
    assert!(matches!(err, RustStreamError::Dispatch(_)), "got {err:?}");
    assert!(
        matches!(health.state(), HealthState::Failed { .. }),
        "the terminal state stays Failed after shutdown",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_taken_after_the_fail_fast_still_sees_failed() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(explode));

    let running = app.start().await.expect("startup failed");
    // Deliberately no probe yet: the transition must be stored even with zero subscribers,
    // because in the real deployment the healthz task may come up after the failure.
    publisher
        .publish(OutgoingMessage::new("health.boom", &order_bytes(1)))
        .await
        .expect("publish failed");
    running.stopping().await;
    // The watcher's send is unobservable without a subscriber, and subscribing early would
    // itself keep the transition alive - so a real-time barrier is the only way to let it fire
    // against zero probes. The single read below must then see the stored state; waiting on
    // `changed()` instead would mask a transition that was dropped for lack of subscribers.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let health = running.health();
    match health.state() {
        HealthState::Failed { reason } => assert!(
            reason.contains("health.boom"),
            "the reason must name the failing subscription, got: {reason}",
        ),
        other => panic!(
            "a probe subscribed after the fail-fast must still observe Failed, got {other:?}",
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_probe_keeps_the_last_state_and_parks() {
    let broker = MemoryBroker::new();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(fine));

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();
    // Detach: dropping the handle never blocks (crate rule), so no transition ever arrives.
    drop(running);

    assert!(
        health.is_running(),
        "a detached service keeps its last state"
    );
    let waited = tokio::time::timeout(Duration::from_millis(200), health.changed()).await;
    assert!(
        waited.is_err(),
        "changed() must park forever instead of spinning when no transition can arrive",
    );
}
