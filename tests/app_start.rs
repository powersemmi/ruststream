//! Integration tests for the background-start handle: `RustStream::start` -> `RunningApp`.
//! Driven over `MemoryBroker`.
//!
//! These tests exercise the run machinery itself (startup, readiness, fail-fast, teardown), which
//! is exactly what the `TestApp` harness bypasses by design - hence the raw broker + publisher
//! wiring. `start()` resolves only after subscriptions are open, so a single publish suffices;
//! no republish loops.
#![cfg(feature = "macros")]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, RustStreamError};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

// Notifies keyed per handler so the parallel tests do not interfere; each handler is used by one
// test only. `notify_one` stores a permit, so the handler may fire before the test awaits.
static SEEN: Notify = Notify::const_new();
static TRAIT_SEEN: Notify = Notify::const_new();

#[subscriber("started.orders")]
async fn observe(_order: &Order) -> HandlerResult {
    SEEN.notify_one();
    HandlerResult::Ack
}

#[subscriber("started.trait")]
async fn observe_trait(_order: &Order) -> HandlerResult {
    TRAIT_SEEN.notify_one();
    HandlerResult::Ack
}

/// Default policy: a panic fails fast, tearing the started service down.
#[subscriber("started.boom")]
async fn boom(order: &Order) -> HandlerResult {
    // The test publishes ids other than u32::MAX, so this assertion always fails (panics); the
    // trailing expression keeps the body typed as HandlerResult.
    assert_eq!(order.id, u32::MAX, "handler exploded");
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_resolves_running_and_shutdown_completes() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(broker, |b| b.include(observe));

    // --8<-- [start:handle]
    // `start` resolves only once subscriptions are open, so one publish is guaranteed to land.
    let running = app.start().await.expect("startup failed");
    publisher
        .publish(OutgoingMessage::new("started.orders", &order_bytes(1)))
        .await
        .expect("publish failed");
    tokio::time::timeout(Duration::from_secs(5), SEEN.notified())
        .await
        .expect("handler never saw the message");

    running.shutdown().await.expect("graceful shutdown failed");
    // --8<-- [end:handle]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_resolves_on_fail_fast_and_shutdown_surfaces_it() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(broker, |b| b.include(boom));

    let running = app.start().await.expect("startup failed");

    // `stopping()` stays pending while the service is healthy and resolves when the panicking
    // handler triggers the fail-fast teardown.
    publisher
        .publish(OutgoingMessage::new("started.boom", &order_bytes(1)))
        .await
        .expect("publish failed");
    tokio::time::timeout(Duration::from_secs(5), running.stopping())
        .await
        .expect("fail-fast never triggered");

    // The teardown reason survives until shutdown, where it surfaces as a dispatch error.
    let err = running
        .shutdown()
        .await
        .expect_err("fail-fast must surface");
    assert!(matches!(err, RustStreamError::Dispatch(_)), "got: {err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_and_shutdown_run_lifecycle_hooks_in_order() {
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let (o1, o2, o3, o4) = (
        Arc::clone(&order),
        Arc::clone(&order),
        Arc::clone(&order),
        Arc::clone(&order),
    );

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .on_startup(async move |()| {
            o1.lock().expect("poisoned").push("startup");
            Ok::<String, Infallible>("state".to_owned())
        })
        .after_startup(async move |_state: Arc<String>| {
            o2.lock().expect("poisoned").push("after_startup");
            Ok::<(), Infallible>(())
        })
        .on_shutdown(async move |_state: Arc<String>| {
            o3.lock().expect("poisoned").push("on_shutdown");
            Ok::<(), Infallible>(())
        })
        .after_shutdown(async move |state: Arc<String>| {
            // The handle carries the state into the shutdown hooks bound at start time.
            assert_eq!(state.as_str(), "state");
            o4.lock().expect("poisoned").push("after_shutdown");
            Ok::<(), Infallible>(())
        })
        .with_broker(MemoryBroker::new(), |_b| {});

    let running = app.start().await.expect("startup failed");
    assert_eq!(
        *order.lock().expect("poisoned"),
        vec!["startup", "after_startup"],
        "start returns with the startup hooks already run",
    );

    running.shutdown().await.expect("graceful shutdown failed");
    assert_eq!(
        *order.lock().expect("poisoned"),
        vec!["startup", "after_startup", "on_shutdown", "after_shutdown"],
    );
}

// The builder hides behind `impl App`, the way `#[ruststream::app]` services are written.
fn service(broker: MemoryBroker) -> impl App {
    RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(observe_trait);
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_is_reachable_through_the_app_trait() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let running = service(broker).start().await.expect("startup failed");
    publisher
        .publish(OutgoingMessage::new("started.trait", &order_bytes(7)))
        .await
        .expect("publish failed");
    tokio::time::timeout(Duration::from_secs(5), TRAIT_SEEN.notified())
        .await
        .expect("handler never saw the message");
    running.shutdown().await.expect("graceful shutdown failed");
}

/// State-generic no-op subscriber for the lifecycle-hooks test below.
#[subscriber("started.quiet")]
async fn quiet(_order: &Order) -> HandlerResult {
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_hooks_run_and_shutdown_hook_errors_only_log() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(42_u32))
        .after_startup(async move |state| {
            assert_eq!(*state, 42);
            Ok::<_, Infallible>(())
        })
        .on_shutdown(async move |_state| Err::<(), _>(std::io::Error::other("on_shutdown boom")))
        .after_shutdown(async move |_state| {
            Err::<(), _>(std::io::Error::other("after_shutdown boom"))
        })
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(broker, |b| b.include(quiet));

    let running = app.start().await.expect("startup failed");
    assert!(format!("{running:?}").contains("RunningApp"));
    // Shutdown hooks may fail; per the lifecycle contract their errors are logged, never
    // propagated, so the graceful path still completes under the configured timeout.
    running.shutdown().await.expect("hook errors must only log");
}

#[test]
#[should_panic(expected = "on_startup must be called before lifecycle hooks")]
fn on_startup_after_a_lifecycle_hook_panics() {
    // A hook registered first closes over the previous state type and cannot be carried across
    // the state change, so the builder refuses loudly instead of dropping it silently.
    let _app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state| Ok::<_, Infallible>(()))
        .on_startup(async move |()| Ok::<_, Infallible>(42_u32));
}
