//! A part of a subscription that panics outside its handler fails the service while it runs: the
//! dispatch loop (a broker stream that panics) or one of its workers (a context that cannot be
//! built, which dispatch builds before the handler runs). The service stops on its own, the probe
//! reports `Failed`, and `run` and `shutdown` return the failure naming the subscription. A panic
//! inside the handler stays the handler's: its failure policy settles it.
//!
//! The subject is the runner's own reaction to the panic, so these suites start the app with
//! `start` and `run_until` rather than the harness.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

use crate::common;
use crate::log_capture;

use std::convert::Infallible;
use std::future::{pending, ready};
use std::io;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use common::Order;
use futures::Stream;
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublisher,
};
use ruststream::prelude::*;
use ruststream::runtime::{HealthProbe, HealthState, RustStreamError};
use ruststream::{
    BrokerMoves, BuildContext, ConnectedBroker, ContextField, DescribeServer, ServerSpec,
    Subscriber, SubscriptionSource,
};
use tokio::sync::Notify;
use tokio::time::timeout;

/// How long a service that lost a part of a subscription may take to stop on its own. A service
/// that ran on would run on forever, so the bound is what turns that into a failed assertion.
const STOPS_WITHIN: Duration = Duration::from_secs(5);

/// A queue whose consumer panics the first time the dispatch loop reads it: a client library
/// that broke under the loop.
#[derive(Debug, Clone, Copy)]
struct BrokenQueue;

impl BrokenQueue {
    const fn new() -> Self {
        Self
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for BrokenQueue {
    type Subscriber = BrokenConsumer;
    type Copies = BrokerMoves;

    fn name(&self) -> &'static str {
        "watch.stream"
    }

    fn subscribe(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<BrokenConsumer, MemoryError>> {
        ready(Ok(BrokenConsumer))
    }
}

struct BrokenConsumer;

struct BrokenStream;

impl Stream for BrokenStream {
    type Item = Result<MemoryMessage, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        panic!("the client library broke");
    }
}

impl Subscriber for BrokenConsumer {
    type Message = MemoryMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<MemoryMessage, Infallible>> + Send + '_ {
        BrokenStream
    }
}

#[subscriber(BrokenQueue::new())]
async fn streamed(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// A broker that connects and then refuses to shut down.
struct Stubborn;

/// The connected form of [`Stubborn`].
struct ConnectedStubborn;

impl Broker for Stubborn {
    type Error = io::Error;
    type Connected = ConnectedStubborn;

    fn connect(self) -> impl Future<Output = Result<ConnectedStubborn, io::Error>> {
        ready(Ok(ConnectedStubborn))
    }
}

impl ConnectedBroker for ConnectedStubborn {
    type Error = io::Error;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), io::Error>> {
        ready(Err(io::Error::other("teardown refused")))
    }
}

// Self-describing, so it registers under the label the teardown's log names it by.
impl DescribeServer for Stubborn {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new("stubborn:0", "memory")
    }
}

/// A delivery's context that cannot be built: dispatch builds it on the part of the subscription
/// that took the delivery, before the handler's guard.
struct Unbuildable;

impl BuildContext<MemoryMessage> for Unbuildable {
    fn build(_msg: &MemoryMessage) -> Self {
        panic!("the delivery's context could not be built");
    }
}

/// A key of [`Unbuildable`], so a handler that takes it makes that the subscription's context.
#[derive(Clone, Copy, Default)]
struct Built;

impl ContextField for Built {
    type Context = Unbuildable;
    type Value = ();

    fn read(self, _src: &Unbuildable) {}
}

#[subscriber("watch.workers", workers(2))]
async fn pooled(_order: &Order, Ctx(()): Ctx<Built>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("watch.keyed", workers(2, by_key))]
async fn keyed(_order: &Order, Ctx(()): Ctx<Built>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("watch.threads", threads(2))]
async fn threaded(_order: &Order, Ctx(()): Ctx<Built>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("watch.threads_keyed", threads(2, by_key))]
async fn threaded_keyed(_order: &Order, Ctx(()): Ctx<Built>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Waits until the probe reports a failure, and hands back its reason. The probe wakes on every
/// change, the subscription count included, so a wake-up may still read `Running`.
async fn until_failed(health: &mut HealthProbe) -> String {
    timeout(STOPS_WITHIN, async {
        loop {
            if let HealthState::Failed { reason } = health.state() {
                break reason;
            }
            health.changed().await;
        }
    })
    .await
    .expect("the probe never reported the failure")
}

/// Starts `app`, publishes one order to `subject`, and waits for the service to stop on its own:
/// nothing here calls `shutdown` before it has. Hands back the probe's reason and the error
/// `shutdown` returned afterwards.
async fn failure_of(
    app: impl App,
    publisher: &MemoryPublisher,
    subject: &'static str,
) -> (String, RustStreamError) {
    let running = app.start().await.expect("startup failed");
    let mut health = running.health();
    publisher
        .message(&Order { id: 1 })
        .to(subject)
        .publish()
        .await
        .expect("publish failed");
    timeout(STOPS_WITHIN, running.stopping())
        .await
        .unwrap_or_else(|_| panic!("`{subject}` lost a worker and the service ran on"));
    let reason = until_failed(&mut health).await;
    let error = running
        .shutdown()
        .await
        .expect_err("the teardown returns the failure");
    (reason, error)
}

/// The failure a part of `subject` reported: the probe and `shutdown` name the subscription and
/// the part, and say the same thing.
fn assert_names(subject: &str, part: &str, reason: &str, error: &RustStreamError) {
    let expected = format!("{subject}: {part} panicked");
    assert_eq!(reason, expected, "the probe's reason");
    match error {
        RustStreamError::Dispatch(failure) => assert_eq!(failure, &expected, "the error"),
        other => panic!("expected the dispatch failure of `{subject}`, got {other:?}"),
    }
}

/// The failure one of the two dedicated threads of `subject` reported. The order goes to the
/// first thread, and the broker hands the delivery that thread dropped to the other one while the
/// service stops, so either may be the one on record.
fn assert_a_thread_named(subject: &str, reason: &str, error: &RustStreamError) {
    let part = ["its dedicated thread 0", "its dedicated thread 1"]
        .into_iter()
        .find(|part| reason == format!("{subject}: {part} panicked"))
        .unwrap_or_else(|| panic!("`{reason}` names no thread of `{subject}`"));
    assert_names(subject, part, reason, error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dispatch_loop_that_panics_fails_the_run() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(streamed));

    // Nothing but the panic can stop it: the caller's own shutdown never comes.
    let error = timeout(STOPS_WITHIN, app.run_until(pending()))
        .await
        .expect("the subscription's loop died and the service ran on")
        .expect_err("a loop that panicked fails the run");

    match error {
        RustStreamError::Dispatch(reason) => {
            assert_eq!(reason, "watch.stream: its dispatch loop panicked");
        }
        other => panic!("expected the dispatch failure, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dispatch_loop_that_panics_flips_the_probe_while_the_service_runs() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(streamed));

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();
    timeout(STOPS_WITHIN, running.stopping())
        .await
        .expect("the subscription's loop died and the service ran on");
    let reason = until_failed(&mut health).await;
    let error = running
        .shutdown()
        .await
        .expect_err("the teardown returns the failure");

    assert_names("watch.stream", "its dispatch loop", &reason, &error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_worker_that_panics_fails_the_service_while_it_runs() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(pooled));

    let (reason, error) = failure_of(app, &publisher, "watch.workers").await;

    assert_names("watch.workers", "a worker", &reason, &error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_worker_that_panics_fails_the_service_while_it_runs() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(keyed));

    let (reason, error) = failure_of(app, &publisher, "watch.keyed").await;

    assert_names("watch.keyed", "a worker", &reason, &error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dedicated_thread_that_panics_fails_the_service_while_it_runs() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(threaded));

    let (reason, error) = failure_of(app, &publisher, "watch.threads").await;

    assert_a_thread_named("watch.threads", &reason, &error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_dedicated_thread_that_panics_fails_the_service_while_it_runs() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(threaded_keyed));

    let (reason, error) = failure_of(app, &publisher, "watch.threads_keyed").await;

    assert_a_thread_named("watch.threads_keyed", &reason, &error);
}

/// The failure that stopped the service is what `run` returns, even when a broker then refuses to
/// close: the refusal is logged where it happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loop_that_panics_is_the_answer_of_the_run_when_a_broker_then_refuses_to_close() {
    log_capture::record_shared();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .logger(TracingLogger)
        .with_broker(MemoryBroker::new(), |b| b.include(streamed))
        .with_broker_labeled("watch-stubborn", Stubborn, |b| b);

    let error = timeout(STOPS_WITHIN, app.run_until(pending()))
        .await
        .expect("the subscription's loop died and the service ran on")
        .expect_err("a loop that panicked fails the run");

    match error {
        RustStreamError::Dispatch(reason) => {
            assert_eq!(reason, "watch.stream: its dispatch loop panicked");
        }
        other => panic!("expected the dispatch failure, got {other:?}"),
    }
    let logged = log_capture::shared_text();
    let markers = [
        "broker shutdown failed",
        r#"broker="watch-stubborn""#,
        "teardown refused",
    ];
    assert!(
        logged
            .lines()
            .any(|line| markers.iter().all(|marker| line.contains(marker))),
        "the refusal is logged with its broker and its cause:\n{logged}",
    );
}

/// `shutdown` answers the same, and the probe keeps the failure that stopped the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loop_that_panics_is_the_answer_of_the_shutdown_when_a_broker_then_refuses_to_close() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(streamed))
        .register_broker(Stubborn);

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();
    timeout(STOPS_WITHIN, running.stopping())
        .await
        .expect("the subscription's loop died and the service ran on");
    let reason = until_failed(&mut health).await;
    let error = running
        .shutdown()
        .await
        .expect_err("the teardown returns the failure");

    assert_names("watch.stream", "its dispatch loop", &reason, &error);
    assert_eq!(
        health.state(),
        HealthState::Failed { reason },
        "the probe's last state"
    );
}

/// Every call of [`fragile`] reports here before it settles.
static CALLED: Notify = Notify::const_new();

/// Panics on the first order and acks the rest, under a policy that settles a panic.
#[subscriber("watch.settled", workers(2), on_failure(panic = drop))]
async fn fragile(order: &Order) -> HandlerOutcome {
    CALLED.notify_one();
    assert_ne!(order.id, 1, "the handler broke");
    HandlerOutcome::ack()
}

/// A panic inside the handler is the handler's: the policy settles the delivery, and the
/// subscription consumes on as the service runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_panic_under_a_settling_policy_leaves_the_service_running() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(fragile));

    let running = app.start().await.expect("startup failed");
    let health = running.health();
    // The second order reaches the handler after the first one panicked: the subscription
    // consumes on.
    for id in [1, 2] {
        publisher
            .message(&Order { id })
            .to("watch.settled")
            .publish()
            .await
            .expect("publish failed");
        timeout(STOPS_WITHIN, CALLED.notified())
            .await
            .unwrap_or_else(|_| panic!("order {id} never reached the handler"));
    }

    // The teardown waits for the handlers, so a failure either of them raised is in by now.
    running
        .shutdown()
        .await
        .expect("a handler panic the policy settled is not a failure of the service");
    assert_eq!(health.state(), HealthState::Stopped);
}
