//! The edges of the [`TestApp`](ruststream::testing::TestApp) harness that the happy-path suite in
//! `tests/testing_harness.rs` never reaches: addressing a broker that is not there (or is there
//! twice), a broker registered for its lifecycle only and therefore carrying no in-process
//! transport, the unscoped injection entry points, the post-settle drain, and the teardown under a
//! configured shutdown timeout.
//!
//! The mistakes a test author makes while addressing brokers are panics, not errors, so the cases
//! that name them are `should_panic` and assert on the message the author reads.
#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::future::{Future, ready};
use std::sync::Mutex;
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Broker, ConnectedBroker, Deserialized, Outgoing, Serialized, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Serialize, Deserialize, PartialEq, Debug, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The payload view the byte-injection case below takes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// Bytes injected as themselves: what that case sends, since its payload is a frame rather than
/// a model. It declares no name, so the injection names its subject.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

#[subscriber("orders")]
async fn handle_orders(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Panics on id 0 (the deliberate negative-test trigger) under the default `panic = fail_fast`:
/// the service tears itself down, which is what `assert_running` then has to report.
#[subscriber("boom")]
async fn always_panics(order: &Order) -> HandlerOutcome {
    assert!(order.id != 0, "boom on id 0");
    HandlerOutcome::ack()
}

#[subscriber("frames")]
async fn ingest(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0.len();
    HandlerOutcome::ack()
}

// --- A broker with no in-process test transport. ---

/// A broker registered for its lifecycle only. Nothing declares it with
/// `register_testable_broker!`, so the harness finds no [`TestableBroker`] view behind its
/// connected form - the shape every real broker has in a build where its own feature is off.
///
/// [`TestableBroker`]: ruststream::testing::TestableBroker
#[derive(Debug)]
struct Opaque;

/// The connected form of [`Opaque`]. It reaches the harness erased, exactly like a registered
/// broker's, and is what the registration lookup fails to resolve.
#[derive(Debug)]
struct ConnectedOpaque;

#[derive(Debug, thiserror::Error)]
#[error("the opaque broker performs no I/O")]
struct OpaqueError;

impl Broker for Opaque {
    type Error = OpaqueError;
    type Connected = ConnectedOpaque;

    fn connect(self) -> impl Future<Output = Result<ConnectedOpaque, OpaqueError>> {
        ready(Ok(ConnectedOpaque))
    }
}

impl ConnectedBroker for ConnectedOpaque {
    type Error = OpaqueError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), OpaqueError>> {
        ready(Ok(()))
    }
}

/// A broker the harness cannot reach into says so by name, instead of reporting an empty run: an
/// injection through it fails with [`TestError::NoTransport`], and its publish log reads as empty
/// because nothing recorded it, not because nothing was published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_without_an_in_process_transport_is_reported_by_name() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .register_broker(Opaque)
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.expect("start");

    let opaque = tb.broker::<Opaque>();
    assert!(matches!(
        opaque.publish("orders", &Order { id: 1 }).await,
        Err(TestError::NoTransport(_)),
    ));
    opaque.published::<Order>("orders").assert_not_called();

    // Two brokers are registered, so the unscoped convenience has no single target to pick.
    assert!(matches!(
        tb.publish("orders", &Order { id: 1 }).await,
        Err(TestError::Ambiguous),
    ));

    tb.shutdown().await.expect("shutdown");
}

/// Addressing a broker type the app never registered is a test-authoring mistake, so the panic
/// names the type the author asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "no registered broker of type")]
async fn addressing_an_unregistered_broker_type_names_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(Opaque);
    let tb = TestApp::start(app).await.expect("start");

    let _ = tb.broker::<MemoryBroker>();
}

/// The same mistake while building a mirror state: the builder's broker view reports it the same
/// way, because that is where a wrong publisher would otherwise be wired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "no registered broker of type")]
async fn a_mirror_state_addressing_an_unregistered_broker_type_names_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(Opaque);

    let _ = TestApp::with_state(app, |brokers| {
        let _ = brokers.broker::<MemoryBroker>();
    })
    .await;
}

/// Two brokers of one type give the mirror state's builder no single answer either, and the panic
/// says which type was ambiguous.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "more than one broker of type")]
async fn a_mirror_state_addressing_a_duplicated_broker_type_names_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker_labeled("a", MemoryBroker::new(), |b| b.include(handle_orders))
        .with_broker_labeled("b", MemoryBroker::new(), |b| b.include(ingest));

    let _ = TestApp::with_state(app, |brokers| {
        let _ = brokers.broker::<MemoryBroker>();
    })
    .await;
}

/// The unscoped entry point picks the sole broker, so an injection into a single-broker app needs
/// no addressing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_unscoped_injection_picks_the_sole_broker() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(ingest));
    let tb = TestApp::start(app).await.expect("start");

    tb.message(&Wire(b"frame".to_vec()))
        .to("frames")
        .publish()
        .await
        .expect("inject");

    tb.broker::<MemoryBroker>()
        .subscriber("frames")
        .assert_called_once()
        .with_raw(b"frame")
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// --- The post-settle drain. ---

/// Releases the continuation below. The handler's post-settle work is deliberately still pending
/// when the settle returns, so `drain` is the only thing that can make it finish.
static RELEASE: Notify = Notify::const_new();

/// What the released continuation recorded, for the assertion that it actually ran.
static DRAINED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

#[subscriber("gated")]
async fn gated(order: &Order) -> HandlerOutcome {
    let id = order.id;
    HandlerOutcome::ack().and_after(async move {
        RELEASE.notified().await;
        DRAINED
            .lock()
            .expect("the test holds no poisoned lock")
            .push(id);
    })
}

/// `settle` returns once the deliveries are settled, which says nothing about the post-settle
/// continuations they spawned; `drain` is what waits for those. A single-threaded runtime is what
/// makes the ordering exact: the continuation cannot finish while the test itself is running.
#[tokio::test]
async fn drain_waits_for_a_still_pending_post_settle_continuation() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(gated));
    let tb = TestApp::start(app).await.expect("start");

    tb.publish("gated", &Order { id: 3 })
        .await
        .expect("publish");
    assert!(
        DRAINED
            .lock()
            .expect("the test holds no poisoned lock")
            .is_empty(),
        "the continuation must still be pending when the settle returns",
    );

    // The permit is stored, so the continuation is runnable but has not run: only the drain's own
    // yielding lets it finish.
    RELEASE.notify_one();
    tb.drain().await;

    assert_eq!(
        DRAINED
            .lock()
            .expect("the test holds no poisoned lock")
            .as_slice(),
        [3],
    );
    tb.shutdown().await.expect("shutdown");
}

// --- Startup hooks and teardown. ---

/// What the `after_startup` hook recorded, proving the harness runs it rather than skipping to the
/// dispatch loops.
static STARTED: Mutex<bool> = Mutex::new(false);

/// The harness runs `after_startup` after the subscriptions are open, and a configured shutdown
/// timeout bounds the teardown instead of waiting for the dispatch loops indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_harness_runs_after_startup_and_honours_the_shutdown_timeout() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .after_startup(async move |_state| {
            *STARTED.lock().expect("the test holds no poisoned lock") = true;
            Ok::<_, std::convert::Infallible>(())
        })
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.expect("start");

    assert!(*STARTED.lock().expect("the test holds no poisoned lock"));
    tb.publish("orders", &Order { id: 1 })
        .await
        .expect("publish");

    tb.shutdown().await.expect("shutdown");
}

/// A failing `after_startup` hook aborts the harness the same way a failing `on_startup` one does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_after_startup_hook_is_reported_as_a_startup_error() {
    #[derive(Debug, thiserror::Error)]
    #[error("the readiness signal never landed")]
    struct NotReady;

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state| Err::<(), _>(NotReady))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));

    match TestApp::start(app).await {
        Err(TestError::Startup(source)) => {
            assert!(source.to_string().contains("readiness signal"));
        }
        other => panic!("expected a startup error, got {:?}", other.map(|_| ())),
    }
}

/// `assert_running` on a torn-down service reports the failure that tore it down, so the test
/// author sees the cause rather than a bare assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "expected the service to be running")]
async fn assert_running_reports_why_the_service_stopped() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(always_panics));
    let tb = TestApp::start(app).await.expect("start");

    tb.publish("boom", &Order { id: 0 }).await.expect("publish");
    tb.assert_running();
}
