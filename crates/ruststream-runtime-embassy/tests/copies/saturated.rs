//! A store whose records all hold refused copies does not stall the subscription: the broker
//! holds one message, the store one record, and a delivery that fails finds the store full while
//! the copy it waits behind is refused for want of the very room the delivery holds.
//!
//! The handler fails the first delivery of a job (`a` fails twice) with a retry later. The driver
//! task publishes on the local bus, moves embassy-time's mock clock and reads which deliveries the
//! handler saw.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryError, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::{Broker, Deserialized, LocalPublisher, OutgoingMessage, subscriber};
use ruststream_runtime_embassy::Health;
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The delay the handler asks its retries for.
const LATER: Duration = Duration::from_secs(5);

/// A step of the clock longer than the wait before a refused copy is published again.
const RETRY_STEP: Duration = Duration::from_millis(60);

/// The board's bus: each subscription holds one message, waiting or in hand.
type Bus = LocalMemoryBroker<1>;

/// A job as its bytes; the first byte names it.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// How often the handler saw each job, by its first byte.
static SEEN: [AtomicU32; 128] = [const { AtomicU32::new(0) }; 128];

fn seen(job: u8) -> u32 {
    SEEN[usize::from(job)].load(Ordering::Relaxed)
}

/// Fails the first delivery of a job with a retry later, and acks the next one; `a` fails its
/// first two.
#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let name = job.0[0];
    let before = SEEN[usize::from(name)].fetch_add(1, Ordering::Relaxed);
    let failures = if name == b'a' { 2 } else { 1 };
    if before < failures {
        HandlerOutcome::retry_after(LATER)
    } else {
        HandlerOutcome::ack()
    }
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
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

async fn try_publish(bus: &Bus, job: u8) -> Result<(), LocalMemoryError> {
    let connected = bus.clone().connect().await.expect("the bus connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced("bus/jobs", BytesMut::from(&[job][..])),
            None,
        )
        .await
}

async fn publish(bus: &Bus, job: u8) {
    try_publish(bus, job)
        .await
        .expect("the bus takes the message");
}

async fn check_saturation(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    // `a` fails and its copy fills the one record. `b` takes the broker's only place and fails
    // behind it, so when `a`'s copy falls due the broker refuses it.
    publish(&stand.bus, b'a').await;
    settle().await;
    publish(&stand.bus, b'b').await;
    settle().await;
    let waiting = (seen(b'a'), seen(b'b'));
    advance(LATER).await;
    // `b` is given back to the broker, runs again and is acked; the copy of `a` goes through.
    advance(RETRY_STEP).await;
    let after_first = (seen(b'a'), seen(b'b'));

    // `a` fails again and fills the record; `c` fails behind it the same way.
    publish(&stand.bus, b'c').await;
    settle().await;
    advance(LATER).await;
    advance(RETRY_STEP).await;
    let after_second = (seen(b'a'), seen(b'c'));

    assert_eq!(waiting, (1, 1), "deliveries of `a` and `b` while `b` waits");
    assert_eq!(after_first, (2, 2), "deliveries of `a` and `b` once freed");
    assert_eq!(after_second, (3, 2), "deliveries of `a` and `c` once freed");
    passed("a store saturated by refused copies gives the waiting delivery back to the broker");

    // `d` fails and its copy fills the record; `e` fails behind it and waits. A stop reaches the
    // waiting delivery at once, without any time passing, and the app ends once the copy of `d`
    // has left.
    publish(&stand.bus, b'd').await;
    settle().await;
    publish(&stand.bus, b'e').await;
    settle().await;
    app::CONTROL.stop();
    settle().await;
    let after_stop = try_publish(&stand.bus, b'z').await.is_ok();
    advance(LATER).await;
    advance(RETRY_STEP).await;
    let health = app::CONTROL.health();
    assert!(
        after_stop,
        "the place the waiting delivery held is free once the app is told to stop"
    );
    assert_eq!(
        health,
        Health::Stopped,
        "the app after a stop with a waiting delivery"
    );
    assert_eq!(seen(b'e'), 1, "deliveries of `e`: none after the stop");
    passed("a stop ends the wait for a record");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_saturation(spawner, stand).await;
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
