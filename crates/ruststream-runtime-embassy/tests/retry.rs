//! `retry_after` on the embedded runner: a device service whose handler asks for a later retry,
//! its copies kept in the store the mount site sized with `.retry_capacity::<CAP>()`.
//!
//! The handler fails the first delivery of every payload and acks the next. The driver task
//! publishes on the local bus, moves embassy-time's mock clock and reads which deliveries the
//! handler saw, and how many allocations one failure made.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::alloc::System;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::{
    Deserialized, HeaderMap, LocalBroker, LocalPublisher, OutgoingMessage, Str, subscriber,
};
use static_cell::StaticCell;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The delay the handler asks its retries for.
const LATER: Duration = Duration::from_secs(5);

/// A second short of [`LATER`].
const ALMOST: Duration = Duration::from_secs(4);

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job as its bytes; the first byte names it.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// How often the handler saw each job, by its first byte.
static SEEN: [AtomicU32; 128] = [const { AtomicU32::new(0) }; 128];

fn seen(job: u8) -> u32 {
    SEEN[usize::from(job)].load(Ordering::Relaxed)
}

/// Fails the first delivery of a job with a retry later, and acks the next one. A job named
/// `o` always succeeds.
#[subscriber(LocalMemorySource::new("bus/jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let name = job.0[0];
    let before = SEEN[usize::from(name)].fetch_add(1, Ordering::Relaxed);
    if before == 0 && name != b'o' {
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

/// Allocations one delivery of `job` makes on its way through the app, its publish excluded.
async fn allocations_of(bus: &Bus, job: u8) -> usize {
    publish(bus, job).await;
    let delivery = Region::new(ALLOCATOR);
    settle().await;
    delivery.change().allocations
}

/// Allocations of the one part of a copy the store does not keep in place: the delivery's
/// header map, carried on with the retry count in it.
fn allocations_of_the_copy_headers() -> usize {
    let headers = Region::new(ALLOCATOR);
    // The delivery's map is empty, as the bus delivers it: cloning it allocates nothing.
    let mut copy = HeaderMap::new();
    copy.insert(Str::from_static(RETRY_COUNT_HEADER), 1_u64.to_string());
    drop(copy);
    headers.change().allocations
}

async fn check_retries(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    // A copy waits for its delay on the clock, then reaches the subscription again.
    publish(&stand.bus, b'w').await;
    settle().await;
    let before_due = seen(b'w');
    advance(ALMOST).await;
    let almost_due = seen(b'w');
    advance(Duration::from_secs(1)).await;
    let after_due = seen(b'w');

    // The store holds its records from the start: past the first copy, a failure allocates only
    // what the copy carries on.
    let acked = allocations_of(&stand.bus, b'o').await;
    let failed = allocations_of(&stand.bus, b'f').await;
    let carried = allocations_of_the_copy_headers();

    // The one record holds `f`'s copy, so `g`'s failure finds the store full and is requeued.
    publish(&stand.bus, b'g').await;
    settle().await;
    let requeued = seen(b'g');
    let kept = seen(b'f');
    advance(LATER).await;
    let fired = seen(b'f');

    app::CONTROL.stop();
    settle().await;

    assert_eq!(before_due, 1, "deliveries of `w` before its copy is due");
    assert_eq!(
        almost_due, 1,
        "deliveries of `w` a second before its copy is due"
    );
    assert_eq!(after_due, 2, "deliveries of `w` once its copy fell due");
    passed("a kept copy fires on the clock and reaches the subscription");
    assert_eq!(
        failed - acked,
        carried,
        "allocations of a failure beyond an acked delivery's, against the copy's headers alone"
    );
    passed("the store allocates nothing per failure");
    assert_eq!(
        requeued, 2,
        "deliveries of `g`, requeued when the store was full"
    );
    assert_eq!(kept, 1, "deliveries of `f` while its copy waits");
    assert_eq!(fired, 2, "deliveries of `f` once its copy fell due");
    passed("a full store requeues the delivery");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_retries(spawner, stand).await;
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
