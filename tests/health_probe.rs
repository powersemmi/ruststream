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
