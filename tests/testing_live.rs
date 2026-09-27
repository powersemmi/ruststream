//! The harness in live mode: the same app a test starts in process, against brokers connected
//! through their ordinary `connect`.
//!
//! The in-memory broker stands for a running stand here. Its `connect` is its real connection,
//! the harness installs nothing into it, and a delayed redelivery runs on its own timer, which is
//! the position a networked broker is in.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::convert::Infallible;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{AppInfo, HandlerOutcome, Identity, PublishExt, RustStream};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Broker, Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// Short, so the live tests spend little real time on it; what matters is that it passes on the
/// broker's own timer.
const RETRY_DELAY: Duration = Duration::from_millis(200);

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

/// The service's state: how many orders it has seen.
#[derive(Default)]
struct Seen(AtomicU32);

/// Asks for the first order again after a delay, and answers every later one with a receipt.
#[subscriber("orders", publish)]
async fn accept(order: &Order, ctx: &mut Context<'_, (), Seen>) -> Result<Receipt, HandlerOutcome> {
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(HandlerOutcome::retry_after(RETRY_DELAY));
    }
    Ok(Receipt { id: order.id })
}

#[subscriber("receipts")]
async fn file(receipt: &Receipt) -> HandlerOutcome {
    let _ = receipt.id;
    HandlerOutcome::ack()
}

/// The app `main` would run.
fn app() -> RustStream<Identity, Seen> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Seen::default()))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(accept).out_reply(MemoryPublish);
            b.include(file);
        })
}

/// A first delivery asks to come back, the broker's own timer brings it round after the delay,
/// and the second delivery answers: a live `advance` lets the time pass for real and waits for
/// what fell due.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_redelivery_live() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start_live(app()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .publish()
        .await?;
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY).await?;

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
    tb.broker::<MemoryBroker>()
        .subscriber("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
    tb.shutdown().await?;
    Ok(())
}

// A paused clock would fire a network client's timeouts at once, so the live start refuses it and
// says what to do instead.
#[tokio::test(start_paused = true)]
async fn a_live_start_refuses_a_paused_clock() {
    match TestApp::start_live(app()).await {
        Err(TestError::PausedClock) => {}
        other => panic!(
            "expected the paused clock refused, got {:?}",
            other.map(|_| ())
        ),
    }
}

/// A job, which the test sends wherever it is being run.
#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Job {
    id: u64,
}

/// Holds the delivery longer than the deadline the test starts the harness with.
#[subscriber("slow")]
async fn slow(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    // The scenario is a handler that outlives the deadline, not a wait for synchronisation.
    tokio::time::sleep(Duration::from_secs(2)).await;
    HandlerOutcome::ack()
}

// Past the deadline a live settle stops waiting and says what it was waiting on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_settle_that_outlasts_the_deadline_says_so() {
    let app = RustStream::new(AppInfo::new("slow", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(10))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(slow);
        });
    let tb = TestApp::start_live_within(app, Duration::from_millis(100))
        .await
        .expect("start");

    let err = tb
        .broker::<MemoryBroker>()
        .message(&Job { id: 1 })
        .to("slow")
        .publish()
        .await
        .expect_err("the handler outlives the deadline");
    let message = err.to_string();
    assert!(message.contains("did not settle within 100ms"), "{message}");
    assert!(
        message.contains("subscription \"slow\" handled 0 of 1 deliveries"),
        "{message}"
    );
}

/// Takes its time over an order, so a settle that stopped waiting too soon finds it unhandled.
#[subscriber("orders")]
async fn take_order_slowly(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    // The scenario is a handler that takes time, not a wait for synchronisation.
    tokio::time::sleep(Duration::from_millis(50)).await;
    HandlerOutcome::ack()
}

/// Why a startup hook could not announce itself.
#[derive(Debug, thiserror::Error)]
#[error("the announcement failed: {0}")]
struct AnnounceError(String);

/// Two brokers of one app, each with a slow `orders` subscription; a startup hook of the east
/// scope announces itself on west's `orders`, through a publisher it pairs from a token itself.
fn announcing_app() -> RustStream {
    let west = MemoryBroker::new().bindable();
    let to_west = west.bind(MemoryPublish);
    RustStream::new(AppInfo::new("announcing", "0.1.0"))
        .with_broker_labeled("east", MemoryBroker::new(), |b| {
            b.after_startup(MemoryPublish, async move |_east: MemoryPublisher| {
                let publisher = to_west
                    .live()
                    .await
                    .map_err(|err| AnnounceError(err.to_string()))?;
                publisher
                    .message(&Order { id: 0 })
                    .publish()
                    .await
                    .map_err(|err| AnnounceError(err.to_string()))
            });
            b.include(take_order_slowly);
        })
        .with_broker_labeled("west", west, |b| {
            b.include(take_order_slowly);
        })
}

/// What a startup hook publishes is the service's own publishing, like a publisher it holds: a
/// live harness neither lists it on the hook's broker nor waits on that broker's subscriptions
/// for it, so the settle returns at once rather than at its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_startup_hook_publish_is_not_awaited() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start_live_within(announcing_app(), Duration::from_secs(1)).await?;
    tb.settle().await?;
    tb.broker_named("east")
        .subscriber("orders")
        .assert_not_called();
    tb.broker_named("east")
        .published::<Order>("orders")
        .assert_not_called();
    tb.broker_named("west")
        .published::<Order>("orders")
        .assert_not_called();
    tb.shutdown().await?;
    Ok(())
}

/// Takes an order.
#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Why the start below fails: a readiness check that never passes.
#[derive(Debug, thiserror::Error)]
#[error("the readiness check never passed")]
struct NotReady;

/// An app whose start fails after its broker connected and its subscription opened.
fn failing_app(broker: MemoryBroker) -> RustStream {
    RustStream::new(AppInfo::new("failing", "0.1.0"))
        .after_startup(async move |_state| Err::<(), _>(NotReady))
        .with_broker(broker, |b| {
            b.include(take_order);
        })
}

/// A failing `after_startup` fails the start with the hook's own error, and the start shuts down
/// what it connected, in either mode: the broker refuses a publish afterwards, as it does once the
/// service has shut it down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_start_shuts_its_brokers_down() {
    for live in [false, true] {
        let broker = MemoryBroker::new();
        let app = failing_app(broker.clone());
        let started = if live {
            TestApp::start_live(app).await
        } else {
            TestApp::start(app).await
        };
        match started {
            Err(TestError::Startup(source)) => assert_eq!(
                source.to_string(),
                "the readiness check never passed",
                "the hook's own error is the start's (live: {live})",
            ),
            other => panic!(
                "expected the start to fail (live: {live}), got {:?}",
                other.map(|_| ())
            ),
        }

        let late = broker.publisher().message(&Order { id: 1 }).publish().await;
        assert!(
            late.is_err(),
            "the broker a failed start connected is still up (live: {live})",
        );
    }
}

/// Holds its delivery far longer than any test runs.
#[subscriber("stuck")]
async fn stuck(job: &Job, ctx: &mut Context<'_, (), Arc<()>>) -> HandlerOutcome {
    let _ = (job.id, ctx.state());
    // The scenario is a handler that outlives the shutdown timeout, not a wait for
    // synchronisation.
    tokio::time::sleep(Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// A shutdown that times out on a handler stops it, as the service's own shutdown does: once
/// `shutdown` returns, nothing of the app is left holding its state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_shutdown_stops_the_handler_it_waited_for() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(());
    let held = Arc::clone(&state);
    let app = RustStream::new(AppInfo::new("stuck", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(10))
        .on_startup(async move |()| Ok::<_, Infallible>(held))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(stuck);
        });
    let tb = TestApp::start_live_within(app, Duration::from_millis(100)).await?;
    let unsettled = tb
        .broker::<MemoryBroker>()
        .message(&Job { id: 1 })
        .to("stuck")
        .publish()
        .await;
    assert!(unsettled.is_err(), "the handler outlives the deadline");

    tb.shutdown().await?;
    assert_eq!(
        Arc::strong_count(&state),
        1,
        "the handler the shutdown timed out on is still running",
    );
    Ok(())
}

/// Never finishes the delivery it is handed.
#[subscriber("stuck-a")]
async fn stuck_a(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    std::future::pending::<()>().await;
    HandlerOutcome::ack()
}

/// The same, on a second subscription.
#[subscriber("stuck-b")]
async fn stuck_b(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    std::future::pending::<()>().await;
    HandlerOutcome::ack()
}

/// The shutdown timeout bounds the whole teardown, however many subscriptions it waits for, as it
/// does for the service.
#[tokio::test(start_paused = true)]
async fn one_shutdown_timeout_bounds_every_stuck_subscription() -> Result<(), Box<dyn Error>> {
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
    let app = RustStream::new(AppInfo::new("stuck", "0.1.0"))
        .shutdown_timeout(SHUTDOWN_TIMEOUT)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(stuck_a);
            b.include(stuck_b);
        });
    let tb = TestApp::start(app).await?;
    for name in ["stuck-a", "stuck-b"] {
        // The reaction never settles, so the publish is left once the handler holds the delivery.
        let publish = tb
            .broker::<MemoryBroker>()
            .message(&Job { id: 1 })
            .to(name)
            .publish();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), publish)
                .await
                .is_err()
        );
    }

    let started = tokio::time::Instant::now();
    tb.shutdown().await?;
    assert_eq!(started.elapsed(), SHUTDOWN_TIMEOUT);
    Ok(())
}
