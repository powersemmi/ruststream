//! `out_retry` on the embedded runner: a device service whose handler asks for a later retry,
//! its copy kept in the mount's store and published through the policy and to the destination
//! the mount site named.
//!
//! The driver task publishes on the local bus, moves embassy-time's mock clock and reads which
//! deliveries each handler saw.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::{Deserialized, LocalBroker, LocalPublisher, OutgoingMessage, subscriber};
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The delay the handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// A second short of [`LATER`].
const ALMOST: Duration = Duration::from_secs(4);

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// Deliveries the job handler saw.
static TRIED: AtomicU32 = AtomicU32::new(0);

/// Copies the retry handler saw.
static COPIES: AtomicU32 = AtomicU32::new(0);

/// Fails every delivery with a retry later.
#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    TRIED.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::retry_after(LATER)
}

/// Counts the copies that reached the destination the mount site named.
#[subscriber(LocalMemorySource::new("bus/jobs.later"))]
async fn on_copy(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    COPIES.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_copy)
            .include(on_job)
            .out_retry(LocalMemoryPublish)
            .to("bus/jobs.later")
            .retry_capacity::<1>()
    })
}

/// What the scenario keeps in statics.
struct Stand {
    bus: Bus,
    done: Cell<bool>,
}

/// Lets every task the last step woke run until it parks again.
async fn settle() {
    for _ in 0..SETTLE_ROUNDS {
        yield_now().await;
    }
}

/// Moves the mock clock and lets what fell due run.
async fn advance(by: Duration) {
    MockDriver::get().advance(Ticks::try_from(by).expect("the step fits the clock"));
    settle().await;
}

/// Reports one passed check the way the test harness does.
fn passed(check: &str) {
    println!("test {check} ... ok");
}

async fn publish(bus: &Bus, job: u8) {
    let connected = bus.clone().connect().await.expect("the bus connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced("bus/jobs", BytesMut::from(&[job][..])),
            None,
        )
        .await
        .expect("the bus takes the message");
}

async fn check_out_retry(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    publish(&stand.bus, b'a').await;
    settle().await;
    let first = (
        TRIED.load(Ordering::Relaxed),
        COPIES.load(Ordering::Relaxed),
    );
    advance(ALMOST).await;
    let almost = (
        TRIED.load(Ordering::Relaxed),
        COPIES.load(Ordering::Relaxed),
    );
    advance(Duration::from_secs(1)).await;
    let due = (
        TRIED.load(Ordering::Relaxed),
        COPIES.load(Ordering::Relaxed),
    );

    app::CONTROL.stop();
    settle().await;

    assert_eq!(first, (1, 0), "(tries, copies) after the first delivery");
    assert_eq!(
        almost,
        (1, 0),
        "(tries, copies) a second before the copy is due"
    );
    assert_eq!(
        due,
        (1, 1),
        "(tries, copies) once due: the copy reaches the named destination, not the subscription"
    );
    passed("a copy through out_retry reaches its destination on the clock");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_out_retry(spawner, stand).await;
    stand.done.set(true);
}

fn main() {
    static STAND: StaticCell<Stand> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let stand: &'static Stand = STAND.init(Stand {
        bus: Bus::new(),
        done: Cell::new(false),
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            spawner.spawn(drive(spawner, stand).expect("the driver task is free"));
        },
        || stand.done.get(),
    );
}
