//! A handler that keeps itself runnable never lets the tokio app go idle. On a running clock time
//! passes meanwhile and the shutdown timeout fires; the harness's paused clock moves the same way,
//! so a shutdown ends by its timeout, and a wait with no timeout to end it fails.

use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::memory::prelude::{Broker, Publish, PublishExt};
use ruststream::runtime::{
    AppInfo, Bound, HandlerOutcome, PublishError, RustStream, RustStreamError, State,
};
use ruststream::testing::{TestApp, TestError};
use ruststream::{FromRef, Outgoing, subscriber};
use serde::{Deserialize, Serialize};
use tokio::task;
use tokio::time::{self, Instant};

/// How long the app waits for its handlers at shutdown.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the slow handler waits on its timer.
const SLEEP: Duration = Duration::from_secs(5);

/// A command the test publishes, to the destination it names.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "commands")]
struct Command {
    id: u64,
}

/// A note the test publishes, to the destination it names.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "notes")]
struct Note {
    id: u64,
}

/// Yields in a loop and never ends: the task stays runnable.
#[subscriber("commands")]
async fn on_command(command: &Command) -> HandlerOutcome {
    let _ = command.id;
    loop {
        task::yield_now().await;
    }
}

/// Acknowledges, and leaves a continuation behind that yields in a loop and never ends.
#[subscriber("notes")]
async fn on_note(note: &Note, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = note.id;
    ctx.after_settle(async {
        loop {
            task::yield_now().await;
        }
    });
    HandlerOutcome::ack()
}

/// Waits on a timer of its own, which is all it does.
#[subscriber("slow")]
async fn on_slow(slow: &Slow) -> HandlerOutcome {
    let _ = slow.id;
    time::sleep(SLEEP).await;
    HandlerOutcome::ack()
}

/// A slow job the test publishes, to the destination it names.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "slow")]
struct Slow {
    id: u64,
}

fn busy_app(shutdown_timeout: Option<Duration>) -> RustStream {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"));
    let app = match shutdown_timeout {
        Some(timeout) => app.shutdown_timeout(timeout),
        None => app,
    };
    app.with_broker(MemoryBroker::new(), |b| {
        b.include(on_command).include(on_note).include(on_slow)
    })
}

#[tokio::test(start_paused = true)]
async fn a_publish_the_busy_handler_keeps_from_settling_fails() {
    let tb = TestApp::start(busy_app(Some(SHUTDOWN_TIMEOUT)))
        .await
        .expect("start");

    let published = tb
        .broker::<MemoryBroker>()
        .message(&Command { id: 1 })
        .publish()
        .await;

    assert!(
        matches!(
            published,
            Err(PublishError::Publish {
                source: TestError::KeptRunning { .. },
                ..
            })
        ),
        "the publish returned {published:?}",
    );
    // Not part of what this test checks: the handler must go for the runtime to end.
    let _ = tb.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_settle_the_busy_handler_keeps_from_ending_fails() {
    let tb = TestApp::start(busy_app(Some(SHUTDOWN_TIMEOUT)))
        .await
        .expect("start");
    let _ = tb
        .broker::<MemoryBroker>()
        .message(&Command { id: 1 })
        .publish()
        .await;

    let settled = tb.settle().await;

    assert!(
        matches!(settled, Err(TestError::KeptRunning { .. })),
        "the settle returned {settled:?}",
    );
    let _ = tb.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_shutdown_of_a_busy_handler_ends_by_the_shutdown_timeout() {
    let tb = TestApp::start(busy_app(Some(SHUTDOWN_TIMEOUT)))
        .await
        .expect("start");
    let _ = tb
        .broker::<MemoryBroker>()
        .message(&Command { id: 1 })
        .publish()
        .await;

    let before = Instant::now();
    let stopped = tb.shutdown().await;

    assert!(stopped.is_ok(), "the shutdown returned {stopped:?}");
    let waited = before.elapsed();
    assert!(
        waited >= SHUTDOWN_TIMEOUT,
        "the shutdown ended after {waited:?}, before its timeout",
    );
}

#[tokio::test(start_paused = true)]
async fn a_shutdown_of_a_busy_handler_with_no_timeout_fails() {
    let tb = TestApp::start(busy_app(None)).await.expect("start");
    let _ = tb
        .broker::<MemoryBroker>()
        .message(&Command { id: 1 })
        .publish()
        .await;

    let stopped = tb.shutdown().await;

    assert!(
        matches!(&stopped, Err(RustStreamError::Dispatch(reason)) if reason.contains("did not go idle")),
        "the shutdown returned {stopped:?}",
    );
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "did not go idle")]
async fn a_drain_of_a_busy_continuation_fails() {
    let tb = TestApp::start(busy_app(Some(SHUTDOWN_TIMEOUT)))
        .await
        .expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Note { id: 1 })
        .publish()
        .await
        .expect("publish");

    tb.drain().await;
}

/// A task parked on a timer is not a busy one: the paused clock lands on the timer's deadline,
/// not a tick past it.
#[tokio::test(start_paused = true)]
async fn a_handler_parked_on_a_timer_sees_exactly_its_wait_pass_during_a_settle() {
    let tb = TestApp::start(busy_app(Some(SHUTDOWN_TIMEOUT)))
        .await
        .expect("start");
    let before = Instant::now();

    tb.broker::<MemoryBroker>()
        .message(&Slow { id: 1 })
        .publish()
        .await
        .expect("publish");

    assert_eq!(before.elapsed(), SLEEP);
    tb.shutdown().await.expect("shutdown");
}

/// How often the spinning handler has run, and what `after_shutdown` saw of the broker.
#[derive(Clone)]
struct Ticks(Arc<AtomicU64>);

/// A farewell the `after_shutdown` hook publishes, to show whether the broker still takes it.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "farewells")]
struct Farewell {
    id: u64,
}

#[derive(FromRef)]
struct SpinState {
    ticks: Ticks,
    #[from_ref(skip)]
    farewells: Bound<MemoryBroker, Publish>,
}

/// Counts every turn it takes and never ends.
#[subscriber("spin")]
async fn on_spin(spin: &Spin, State(ticks): State<Ticks>) -> HandlerOutcome {
    let _ = spin.id;
    loop {
        ticks.0.fetch_add(1, Ordering::Relaxed);
        task::yield_now().await;
    }
}

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "spin")]
struct Spin {
    id: u64,
}

/// A failed shutdown still leaves nothing behind: the handler is gone, the broker is shut down
/// and `after_shutdown` has run.
#[tokio::test(start_paused = true)]
async fn a_failed_shutdown_stops_the_handler_closes_the_broker_and_runs_after_shutdown() {
    let ticks = Ticks(Arc::new(AtomicU64::new(0)));
    let after_ran = Arc::new(AtomicBool::new(false));
    let broker_open = Arc::new(AtomicBool::new(true));
    let broker = MemoryBroker::new().bindable();
    let farewells = broker.bind(Publish);
    let (state_ticks, hook_ran, hook_open) = (
        ticks.clone(),
        Arc::clone(&after_ran),
        Arc::clone(&broker_open),
    );
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(SpinState {
                ticks: state_ticks,
                farewells,
            })
        })
        .after_shutdown(async move |state: Arc<SpinState>| {
            let open = match state.farewells.clone().live().await {
                Ok(publisher) => publisher
                    .message(&Farewell { id: 1 })
                    .publish()
                    .await
                    .is_ok(),
                Err(_) => false,
            };
            hook_open.store(open, Ordering::Relaxed);
            hook_ran.store(true, Ordering::Relaxed);
            Ok::<_, Infallible>(())
        })
        .with_broker(broker, |b| b.include(on_spin));
    let tb = TestApp::start(app).await.expect("start");
    let _ = tb
        .broker::<MemoryBroker>()
        .message(&Spin { id: 1 })
        .publish()
        .await;

    let stopped = tb.shutdown().await;

    assert!(
        matches!(&stopped, Err(RustStreamError::Dispatch(reason)) if reason.contains("did not go idle")),
        "the shutdown returned {stopped:?}",
    );
    let at_return = ticks.0.load(Ordering::Relaxed);
    for _ in 0..200 {
        task::yield_now().await;
    }
    assert_eq!(
        ticks.0.load(Ordering::Relaxed),
        at_return,
        "the handler kept running after the shutdown returned",
    );
    assert!(
        after_ran.load(Ordering::Relaxed),
        "after_shutdown did not run"
    );
    assert!(
        !broker_open.load(Ordering::Relaxed),
        "the broker was still open in after_shutdown"
    );
}

/// The state of an app whose `after_startup` hook sets the spinning handler off and then fails.
#[derive(FromRef)]
struct StartupState {
    ticks: Ticks,
    #[from_ref(skip)]
    spins: Bound<MemoryBroker, Publish>,
}

/// A failed start leaves nothing behind: the handler the failing hook set running is gone and the
/// broker is closed, though the app has no shutdown timeout to end the wait.
#[tokio::test(start_paused = true)]
async fn a_failed_startup_stops_the_handler_it_set_running_and_closes_the_broker() {
    let ticks = Ticks(Arc::new(AtomicU64::new(0)));
    let broker = MemoryBroker::new().bindable();
    let spins = broker.bind(Publish);
    let probe = spins.clone();
    let (state_ticks, state_spins) = (ticks.clone(), spins);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(StartupState {
                ticks: state_ticks,
                spins: state_spins,
            })
        })
        .after_startup(async |state: Arc<StartupState>| {
            state
                .spins
                .clone()
                .live()
                .await
                .map_err(io::Error::other)?
                .message(&Spin { id: 1 })
                .publish()
                .await
                .map_err(io::Error::other)?;
            while state.ticks.0.load(Ordering::Relaxed) == 0 {
                task::yield_now().await;
            }
            Err(io::Error::other(
                "the hook failed after it set the handler running",
            ))
        })
        .with_broker(broker, |b| b.include(on_spin));

    let started = TestApp::start(app).await;

    assert!(
        matches!(started, Err(TestError::Startup(_))),
        "the start returned an unexpected result",
    );
    let at_return = ticks.0.load(Ordering::Relaxed);
    for _ in 0..200 {
        task::yield_now().await;
    }
    assert_eq!(
        ticks.0.load(Ordering::Relaxed),
        at_return,
        "the handler kept running after the start failed",
    );
    let open = match probe.live().await {
        Ok(publisher) => publisher.message(&Spin { id: 2 }).publish().await.is_ok(),
        Err(_) => false,
    };
    assert!(!open, "the broker was still open after the start failed");
}
