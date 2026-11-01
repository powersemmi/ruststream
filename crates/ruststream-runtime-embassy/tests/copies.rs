//! The copies of one record allocate nothing once it is warm: the copies published at once reuse
//! the same record's buffers, and a dead-letter copy its broker refuses is lost and gives its
//! record back at once.
//!
//! The driver task fills the dead-letter subscription, so the broker refuses the next copy, moves
//! embassy-time's mock clock past the point where the old design published it again and reads
//! that nothing reaches the handler.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use core::time::Duration;
use std::alloc::System;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::{Broker, Deserialized, OutgoingMessage, Publisher, nonzero, subscriber};
use static_cell::StaticCell;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

// Counts every allocation of the process, so the scenario reads how many the copies made.
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// A step of the clock longer than the wait before a refused copy is published again.
const RETRY_STEP: Duration = Duration::from_millis(60);

/// Copies the scenario puts through one record after it is warm.
const COPIES: u32 = 100;

/// What a warm job allocates from its publish to the dead-letter handler: the delivery's and
/// the copy's header maps, and the broker's copy of the payload. The record's buffers add none, so
/// a change that reallocates one per copy moves this number.
const WARM_JOB_ALLOCATIONS: usize = 3;

/// Messages the dead-letter subscription holds: its four slots and the one its handler is on.
const ROOM: u8 = 4;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// Deliveries the dead-letter handler finished.
static DEAD: AtomicU32 = AtomicU32::new(0);

/// Whether the dead-letter handler waits for [`RELEASE`] before it acks.
static HOLD: AtomicBool = AtomicBool::new(false);

/// Lets the dead-letter handler go on.
static RELEASE: Signal<CriticalSectionRawMutex, ()> = Signal::new();

/// Fails every delivery with a retry.
#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry()
}

/// Takes the dead letters, one at a time while [`HOLD`] is set.
#[subscriber(LocalMemorySource::new("bus/jobs.dead"))]
async fn on_dead(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    if HOLD.load(Ordering::Relaxed) {
        RELEASE.wait().await;
    }
    DEAD.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
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

async fn publish(bus: &Bus, subject: &'static str, job: u8) {
    let connected = bus.clone().connect().await.expect("the bus connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced(subject, BytesMut::from(&[job][..])),
            None,
        )
        .await
        .expect("the bus takes the message");
}

/// Allocations one job makes on its way to the dead-letter handler, its publish excluded.
async fn allocations_of_a_job(bus: &Bus, job: u8) -> usize {
    publish(bus, "bus/jobs", job).await;
    let delivery = Region::new(ALLOCATOR);
    settle().await;
    delivery.change().allocations
}

async fn check_copies(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    // Warm-up: the record's buffers grow to the copy, and the first job is published at once.
    let first = allocations_of_a_job(&stand.bus, b'a').await;
    let warm = allocations_of_a_job(&stand.bus, b'b').await;
    let dead_before = DEAD.load(Ordering::Relaxed);

    // Copies published at once, on the same record: each costs what the warm one did.
    let mut at_once = Vec::new();
    for job in 0..COPIES {
        at_once.push(
            allocations_of_a_job(&stand.bus, b'c' + u8::try_from(job % 8).unwrap_or(0)).await,
        );
    }
    let delivered = DEAD.load(Ordering::Relaxed) - dead_before;

    // The dead-letter subscription is full: the broker refuses the next copy, which is lost.
    HOLD.store(true, Ordering::Relaxed);
    for filler in 0..ROOM {
        publish(&stand.bus, "bus/jobs.dead", filler).await;
        // The handler takes the first and holds it, which leaves the four slots for the rest.
        settle().await;
    }
    settle().await;
    publish(&stand.bus, "bus/jobs", b'x').await;
    settle().await;
    let before_refusals = DEAD.load(Ordering::Relaxed);
    for _ in 0..COPIES {
        advance(RETRY_STEP).await;
    }
    let while_refused = DEAD.load(Ordering::Relaxed);

    // The room frees: the fillers reach the handler, and the refused copy never does.
    HOLD.store(false, Ordering::Relaxed);
    for _ in 0..=ROOM {
        RELEASE.signal(());
        advance(RETRY_STEP).await;
    }
    let after_release = DEAD.load(Ordering::Relaxed);

    app::CONTROL.stop();
    settle().await;

    println!("allocations of the first job: {first}, of a warm one: {warm}");
    assert_eq!(
        warm, WARM_JOB_ALLOCATIONS,
        "allocations of a warm job through the dead letter"
    );
    assert!(
        at_once.iter().all(|&count| count == warm),
        "allocations of each copy published at once, against a warm one's {warm}: {at_once:?}"
    );
    assert_eq!(
        delivered, COPIES,
        "dead letters of the copies published at once"
    );
    passed("copies published at once on one record allocate what a warm one did");
    assert_eq!(
        while_refused, before_refusals,
        "dead letters while the broker refuses the copy"
    );
    assert_eq!(
        after_release,
        before_refusals + u32::from(ROOM),
        "dead letters once the room freed: the fillers alone, the refused copy is lost"
    );
    passed("a copy the broker refuses is lost and never published again");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_copies(spawner, stand).await;
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
