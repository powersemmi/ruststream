//! Integration tests for the [`RunningApp`] health probe: the terminal state survives the
//! consumed app handle, and a fail-fast teardown flips the probe with no `shutdown` call - the
//! process may keep running a sibling HTTP task, which is exactly when the probe matters. The
//! probe also counts the subscriptions that consume, and a subscription whose stream ends while
//! the service runs leaves that count or, under `SubscriptionEnd::FailFast`, tears the service
//! down.
//!
//! The subject IS the started app's probe handle, so every suite here keeps `start()` and its
//! `RunningApp`; the harness hands out no health probe.
//!
//! [`RunningApp`]: ruststream::runtime::RunningApp
#![cfg(feature = "macros")]

mod common;

use std::convert::Infallible;
use std::future::{pending, ready};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt, stream};
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage};
use ruststream::runtime::{
    AppInfo, Context, Handle, HandlerOutcome, HealthProbe, HealthState, IntoSource, PublishExt,
    RustStream, RustStreamError, SubscriptionCount, SubscriptionEnd, subscriber as mount,
};
use ruststream::{BrokerMoves, Subscriber, SubscriptionSource, subscriber};
use tokio::sync::Notify;
use tokio::time;

#[subscriber("health.ok")]
async fn fine(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Default policy: a panic fails fast, tearing the started service down.
#[subscriber("health.boom")]
async fn explode(order: &Order) -> HandlerOutcome {
    // The test publishes ids other than u32::MAX, so this assertion always fails (panics); the
    // trailing expression keeps the body typed as HandlerOutcome.
    assert_eq!(order.id, u32::MAX, "handler exploded");
    HandlerOutcome::ack()
}

/// Waits until the probe reports a failure, and hands back its reason. The probe wakes on every
/// change, the subscription count included, so the first wake-up may still read `Running`.
async fn until_failed(health: &mut HealthProbe) -> String {
    time::timeout(Duration::from_secs(5), async {
        loop {
            if let HealthState::Failed { reason } = health.state() {
                break reason;
            }
            health.changed().await;
        }
    })
    .await
    .expect("the probe never observed the fail-fast teardown")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_reports_stopped() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(fine);
    });

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
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(explode);
    });

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();

    publisher
        .message(&Order { id: 1 })
        .to("health.boom")
        .publish()
        .await
        .expect("publish failed");

    // Nobody calls shutdown here: the fail-fast watcher alone must flip the probe, because in
    // the real deployment the HTTP task is still serving /healthz off it.
    let reason = until_failed(&mut health).await;
    assert!(
        reason.contains("health.boom"),
        "the reason must name the failing subscription, got: {reason}",
    );

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

// A paused clock on the current-thread runtime: the sleep below advances only once every other
// task has run, so it lets the fail-fast watcher fire against zero probes before the probe is
// taken, without waiting on real time.
#[tokio::test(start_paused = true)]
async fn probe_taken_after_the_fail_fast_still_sees_failed() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(explode);
    });

    let running = app.start().await.expect("startup failed");
    // Deliberately no probe yet: the transition must be stored even with zero subscribers,
    // because in the real deployment the healthz task may come up after the failure.
    publisher
        .message(&Order { id: 1 })
        .to("health.boom")
        .publish()
        .await
        .expect("publish failed");
    running.stopping().await;
    // The watcher's send is unobservable without a subscriber, and subscribing early would
    // itself keep the transition alive, so the watcher is let run to quiescence first. The single
    // read below must then see the stored state; waiting on `changed()` instead would mask a
    // transition that was dropped for lack of subscribers.
    time::sleep(Duration::from_secs(1)).await;

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

// Paused time: the wait below ends when the runtime, idle, advances its clock, not after a real
// delay.
#[tokio::test(start_paused = true)]
async fn detached_probe_keeps_the_last_state_and_parks() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(fine);
    });

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();
    // Detach: dropping the handle never blocks (crate rule), so no transition ever arrives.
    drop(running);

    assert!(
        health.is_running(),
        "a detached service keeps its last state"
    );
    let waited = time::timeout(Duration::from_millis(200), health.changed()).await;
    assert!(
        waited.is_err(),
        "changed() must park forever instead of spinning when no transition can arrive",
    );
}

/// A queue the server deleted while the service ran: the server cancelled its consumer, so the
/// subscription's stream ends as soon as the loop reads it.
#[derive(Debug, Clone)]
struct DeletedQueue(&'static str);

impl DeletedQueue {
    const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for DeletedQueue {
    type Subscriber = CancelledConsumer;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.0
    }

    fn subscribe(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<CancelledConsumer, MemoryError>> + Send {
        ready(Ok(CancelledConsumer))
    }
}

/// The consumer the server cancelled: its stream has nothing more to give.
struct CancelledConsumer;

impl Subscriber for CancelledConsumer {
    type Message = MemoryMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<MemoryMessage, Infallible>> + Send + '_ {
        stream::empty()
    }
}

#[subscriber(DeletedQueue::new("health.deleted"))]
async fn orphaned(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("health.pooled", workers(4))]
async fn pooled(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Waits until the probe reports `active` consuming subscriptions.
async fn until_active(health: &mut HealthProbe, active: usize) -> SubscriptionCount {
    time::timeout(Duration::from_secs(5), async {
        while health.subscriptions().active != active {
            health.changed().await;
        }
    })
    .await
    .expect("the probe never reported the expected count");
    health.subscriptions()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_probe_counts_each_subscription_once_whatever_its_workers() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(fine);
        b.include(pooled);
    });

    let running = app.start().await.expect("startup failed");
    let health = running.health();
    let count = health.subscriptions();
    assert_eq!((count.active, count.started), (2, 2));

    running.shutdown().await.expect("graceful shutdown failed");
    let count = health.subscriptions();
    assert_eq!(
        (count.active, count.started),
        (0, 2),
        "a stopped service consumes nothing, and still reports what it started",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_leaves_the_count_and_the_service_running_by_default() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(fine);
        b.include(orphaned);
    });

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();

    let count = until_active(&mut health, 1).await;
    assert_eq!(count.started, 2);
    assert_eq!(
        health.state(),
        HealthState::Running,
        "one subscription ending does not end the service",
    );

    running
        .shutdown()
        .await
        .expect("a stream end under the default setting is not a failure");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_under_fail_fast_stops_the_service() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_subscription_end(SubscriptionEnd::FailFast)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(fine);
            b.include(orphaned);
        });

    // Nothing but the stream end can stop it: the caller's own shutdown never comes.
    let err = time::timeout(Duration::from_secs(5), app.run_until(pending()))
        .await
        .expect("the service never stopped on the stream end")
        .expect_err("a stream end under fail-fast must surface from run");
    match err {
        RustStreamError::Dispatch(reason) => assert!(
            reason.contains("health.deleted"),
            "the error must name the subscription, got: {reason}",
        ),
        other => panic!("expected a dispatch failure, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_under_fail_fast_flips_the_probe() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_subscription_end(SubscriptionEnd::FailFast)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(orphaned);
        });

    let running = app.start().await.expect("startup failed");
    let mut health = running.health();
    time::timeout(Duration::from_secs(5), running.stopping())
        .await
        .expect("the stream end never stopped the service");
    // The watcher flips the probe right after the signal; the wait covers the hop.
    let reason = until_failed(&mut health).await;
    assert!(
        reason.contains("health.deleted"),
        "the reason must name the subscription, got: {reason}",
    );
    running
        .shutdown()
        .await
        .expect_err("the teardown surfaces the failure");
}

/// A queue an `on_shutdown` hook deletes: its consumer is cancelled once the hook says so.
#[derive(Debug, Clone)]
struct DeletedOnShutdown(Arc<Notify>);

impl IntoSource for DeletedOnShutdown {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for DeletedOnShutdown {
    type Subscriber = CancelledOnNotice;
    type Copies = BrokerMoves;

    fn name(&self) -> &'static str {
        "health.deleted_on_shutdown"
    }

    fn subscribe(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<CancelledOnNotice, MemoryError>> + Send {
        ready(Ok(CancelledOnNotice(self.0)))
    }
}

/// The consumer the server cancels when the queue goes: its stream ends on the notice.
struct CancelledOnNotice(Arc<Notify>);

impl Subscriber for CancelledOnNotice {
    type Message = MemoryMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<MemoryMessage, Infallible>> + Send + '_ {
        let notice = Arc::clone(&self.0);
        stream::once(async move { notice.notified().await }).filter_map(|()| ready(None))
    }
}

struct Ignore;

impl Handle<Order> for Ignore {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_during_an_on_shutdown_hook_is_part_of_the_shutdown() {
    let notice = Arc::new(Notify::new());
    let probe: Arc<OnceLock<HealthProbe>> = Arc::new(OnceLock::new());
    let (hook_notice, hook_probe) = (Arc::clone(&notice), Arc::clone(&probe));
    let source = DeletedOnShutdown(notice);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_subscription_end(SubscriptionEnd::FailFast)
        .on_shutdown(async move |_state| {
            // The hook deletes the queue and returns only once the loop has read the end.
            hook_notice.notify_one();
            let mut health = hook_probe
                .get()
                .expect("the probe is set after start")
                .clone();
            until_active(&mut health, 0).await;
            Ok::<(), Infallible>(())
        })
        .with_broker(MemoryBroker::new(), move |b| {
            b.include(mount(source, Ignore).build());
        });

    let running = app.start().await.expect("startup failed");
    probe.set(running.health()).expect("set once");
    running
        .shutdown()
        .await
        .expect("a stream ending inside an on_shutdown hook is not a failure");
}
