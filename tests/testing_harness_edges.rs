//! The edges of the [`TestApp`](ruststream::testing::TestApp) harness that the happy-path suite in
//! `tests/testing_harness.rs` never reaches: an app carrying a broker with no in-process mode,
//! addressing a broker that is not there (or is there twice), the post-settle drain, and the
//! report of a service that tore itself down.
//!
//! The mistakes a test author makes while addressing brokers are panics, not errors, so the cases
//! that name them are `should_panic` and assert on the message the author reads.
#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use ruststream::memory::{MemoryBroker, Retaining};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Broker, ConnectedBroker, Deserialized, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Serialize, Deserialize, PartialEq, Debug, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The payload view of the second subscription an app with two brokers mounts.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

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

// --- A broker with no in-process mode. ---

/// A broker registered for its lifecycle only. Nothing registers it with
/// `register_testable_broker!`, so the harness has no in-process transition to connect it
/// through - the shape every real broker has in a build where its crate's `testing` feature is
/// off.
#[derive(Debug)]
struct Opaque;

/// The connected form of [`Opaque`], which the harness never reaches.
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

/// An app with a broker the harness cannot run in process does not start, and the error names the
/// broker type, instead of the harness connecting it for real or starting a run that could never
/// reach it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_with_a_broker_that_has_no_in_process_mode_does_not_start() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .register_broker(Opaque)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle_orders);
        });

    match TestApp::start(app).await {
        Err(TestError::NoTransport(broker)) => {
            assert!(broker.ends_with("Opaque"), "{broker}");
        }
        other => panic!(
            "expected a missing in-process mode, got {:?}",
            other.map(|_| ())
        ),
    }
}

/// Addressing a broker type the app never registered is a test-authoring mistake, so the panic
/// names the type the author asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "no registered broker of type")]
async fn addressing_an_unregistered_broker_type_names_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle_orders);
    });
    let tb = TestApp::start(app).await.expect("start");

    let _ = tb.broker::<MemoryBroker<Retaining>>();
}

/// The same mistake while building a mirror state: the builder's broker view reports it the same
/// way, because that is where a wrong publisher would otherwise be wired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "no registered broker of type")]
async fn a_mirror_state_addressing_an_unregistered_broker_type_names_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle_orders);
    });

    let _ = TestApp::with_state(app, |brokers| {
        let _ = brokers.broker::<MemoryBroker<Retaining>>();
    })
    .await;
}

/// Two brokers of one type give the mirror state's builder no single answer either, and the panic
/// says which type was ambiguous.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "more than one broker of type")]
async fn a_mirror_state_addressing_a_duplicated_broker_type_names_it() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker_labeled("a", MemoryBroker::new(), |b| {
            b.include(handle_orders);
        })
        .with_broker_labeled("b", MemoryBroker::new(), |b| {
            b.include(ingest);
        });

    let _ = TestApp::with_state(app, |brokers| {
        let _ = brokers.broker::<MemoryBroker>();
    })
    .await;
}

// --- The post-settle drain. ---

/// The gate the continuation below waits at, and what it records once released: application
/// state, the way a service shares anything with its handlers. The handler's post-settle work is
/// deliberately still pending when the settle returns, so `drain` is the only thing that can make
/// it finish.
#[derive(Default)]
struct Gate {
    release: Notify,
    drained: Mutex<Vec<u64>>,
}

impl Gate {
    fn drained(&self) -> Vec<u64> {
        self.drained
            .lock()
            .expect("the test holds no poisoned lock")
            .clone()
    }
}

#[subscriber("gated")]
async fn gated(order: &Order, ctx: &mut Context<'_, (), Arc<Gate>>) -> HandlerOutcome {
    let id = order.id;
    let gate = Arc::clone(ctx.state());
    HandlerOutcome::ack().and_after(async move {
        gate.release.notified().await;
        gate.drained
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
    let gate = Arc::new(Gate::default());
    let state = Arc::clone(&gate);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(gated);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.publish("gated", &Order { id: 3 })
        .await
        .expect("publish");
    assert!(
        gate.drained().is_empty(),
        "the continuation must still be pending when the settle returns",
    );

    // The permit is stored, so the continuation is runnable but has not run: only the drain's own
    // yielding lets it finish.
    gate.release.notify_one();
    tb.drain().await;

    assert_eq!(gate.drained(), [3]);
    tb.shutdown().await.expect("shutdown");
}

// --- Teardown. ---

/// `assert_running` on a torn-down service reports the failure that tore it down, so the test
/// author sees the cause rather than a bare assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "expected the service to be running")]
async fn assert_running_reports_why_the_service_stopped() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(always_panics);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.publish("boom", &Order { id: 0 }).await.expect("publish");
    tb.assert_running();
}
