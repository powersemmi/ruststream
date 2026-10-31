//! `max_attempts` and `dead_letter` on the embedded runner: a device service whose handler
//! always asks for a retry, until the cap sends the delivery to the dead-letter destination
//! through the mount's store.
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
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::{Broker, Deserialized, LocalPublisher, OutgoingMessage, nonzero, subscriber};
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// A step of the clock longer than anything the app waits for.
const LATER: Duration = Duration::from_secs(60);

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// Deliveries the job handler saw.
static TRIED: AtomicU32 = AtomicU32::new(0);

/// Deliveries the dead-letter handler saw.
static DEAD: AtomicU32 = AtomicU32::new(0);

/// Fails every delivery with a retry.
#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    TRIED.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::retry()
}

/// Counts what reached the dead-letter destination.
#[subscriber(LocalMemorySource::new("bus/jobs.dead"))]
async fn on_dead(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    DEAD.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_dead)
            .include(on_job)
            .max_attempts(nonzero!(2u32))
            .dead_letter("bus/jobs.dead")
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

async fn check_dead_letter(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    publish(&stand.bus, b'a').await;
    settle().await;
    let at_cap = (TRIED.load(Ordering::Relaxed), DEAD.load(Ordering::Relaxed));
    advance(LATER).await;
    let after = (TRIED.load(Ordering::Relaxed), DEAD.load(Ordering::Relaxed));

    app::CONTROL.stop();
    settle().await;

    assert_eq!(
        at_cap,
        (2, 1),
        "(tries, dead letters): the requeued second try spends the cap"
    );
    assert_eq!(
        after,
        (2, 1),
        "(tries, dead letters) a minute later: nothing comes back"
    );
    passed("max_attempts stops after the cap and sends the delivery to the dead letter");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_dead_letter(spawner, stand).await;
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
