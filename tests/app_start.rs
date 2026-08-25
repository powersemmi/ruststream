//! Integration tests for the background-start handle: `RustStream::start` -> `RunningApp`.
//! Driven over `MemoryBroker`.
//!
//! These tests exercise the run machinery itself (startup, readiness, fail-fast, teardown), which
//! is exactly what the `TestApp` harness bypasses by design - hence the raw broker + publisher
//! wiring. `start()` resolves only after subscriptions are open, so a single publish suffices;
//! no republish loops.
#![cfg(feature = "macros")]

use std::convert::Infallible;
use std::future::ready;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryError};
use ruststream::runtime::{
    App, AppInfo, Context, HandlerMetadata, HandlerResult, PublishError, PublishExt, RustStream,
    RustStreamError,
};
use ruststream::{Broker, ConnectedBroker, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::timeout;

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
        .raw(&order_bytes(1))
        .to("started.orders")
        .publish()
        .await
        .expect("publish failed");
    timeout(Duration::from_secs(5), SEEN.notified())
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
        .raw(&order_bytes(1))
        .to("started.boom")
        .publish()
        .await
        .expect("publish failed");
    timeout(Duration::from_secs(5), running.stopping())
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
        .raw(&order_bytes(7))
        .to("started.trait")
        .publish()
        .await
        .expect("publish failed");
    timeout(Duration::from_secs(5), TRAIT_SEEN.notified())
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
        .on_shutdown(async move |_state| Err::<(), _>(io::Error::other("on_shutdown boom")))
        .after_shutdown(async move |_state| Err::<(), _>(io::Error::other("after_shutdown boom")))
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

/// Signals for the continuation-drain test: the hook fails only after the continuation is in
/// flight, the continuation parks on `RELEASE`, and `DRAINED` records that it completed.
static HOOK_READY: Notify = Notify::const_new();
static CONT_IN_FLIGHT_HOOK: Notify = Notify::const_new();
static CONT_IN_FLIGHT_TEST: Notify = Notify::const_new();
static RELEASE: Notify = Notify::const_new();
static DRAINED: AtomicBool = AtomicBool::new(false);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_after_startup_waits_for_post_settle_continuations() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    // The two-layer closure form is forced here: an async closure's future would borrow the
    // message and context arguments, and the handler bound needs an owned future.
    let handler = |_msg: &_, _ctx: &mut Context| async {
        HandlerResult::ack().and_after(async {
            CONT_IN_FLIGHT_HOOK.notify_one();
            CONT_IN_FLIGHT_TEST.notify_one();
            RELEASE.notified().await;
            DRAINED.store(true, Ordering::SeqCst);
        })
    };
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state| {
            // Hooks run after subscriptions open, so this signal lets the test publish without
            // racing registration; the hook then fails once the continuation is in flight.
            HOOK_READY.notify_one();
            CONT_IN_FLIGHT_HOOK.notified().await;
            Err::<(), _>(io::Error::other("after_startup boom"))
        })
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("unwind.work");
            b.handle(subscriber, handler, HandlerMetadata::raw("unwind.work"));
        });

    let mut start_task = tokio::spawn(app.start());
    HOOK_READY.notified().await;
    publisher
        .raw(b"go")
        .to("unwind.work")
        .publish()
        .await
        .expect("publish failed");
    CONT_IN_FLIGHT_TEST.notified().await;

    // The continuation is parked: the unwind must wait for it, so start() cannot resolve yet.
    let premature = timeout(Duration::from_millis(300), &mut start_task).await;
    assert!(
        premature.is_err(),
        "start() returned before draining post-settle continuations",
    );
    RELEASE.notify_one();
    let err = start_task
        .await
        .expect("join failed")
        .expect_err("the failing hook must abort startup");
    assert!(matches!(err, RustStreamError::Startup(_)), "got: {err:?}");
    assert!(
        DRAINED.load(Ordering::SeqCst),
        "the continuation must complete before start() returns",
    );
}

/// A broker whose connect always fails, for the partial-startup unwind tests.
struct FailingBroker;

/// Uninhabited connected form: [`FailingBroker::connect`] never produces one.
enum NeverConnected {}

impl Broker for FailingBroker {
    type Error = io::Error;
    type Connected = NeverConnected;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Err(io::Error::other("dial refused")))
    }
}

impl ConnectedBroker for NeverConnected {
    type Error = io::Error;
    type Closed = ();

    // The connected form is uninhabited, so the body diverges: there is no value a `ready(..)`
    // rewrite could carry, only the divergence wrapped in one more layer.
    #[allow(clippy::unused_async_trait_impl)]
    async fn shutdown(self) -> Result<(), Self::Error> {
        match self {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_connect_unwinds_already_connected_brokers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .register_broker(broker)
        .register_broker(FailingBroker);

    let err = app
        .start()
        .await
        .expect_err("the second broker cannot connect");
    assert!(matches!(err, RustStreamError::Connect(_)), "got: {err:?}");
    // The first broker had connected; the unwind must shut it down, not leave it live.
    let err = publisher
        .raw(b"x")
        .to("started.unwind")
        .publish()
        .await
        .expect_err("the unwound broker must reject the publish");
    assert!(matches!(err, PublishError::Publish(MemoryError::ShutDown)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_after_startup_unwinds_connected_brokers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state| Err::<(), _>(io::Error::other("after_startup boom")))
        .with_broker(broker, |b| b.include(quiet));

    let err = app
        .start()
        .await
        .expect_err("the failing hook must abort startup");
    assert!(matches!(err, RustStreamError::Startup(_)), "got: {err:?}");
    // The hook failed after the broker connected and dispatch spawned; both are unwound.
    let err = publisher
        .raw(b"x")
        .to("started.quiet")
        .publish()
        .await
        .expect_err("the unwound broker must reject the publish");
    assert!(matches!(err, PublishError::Publish(MemoryError::ShutDown)));
}
