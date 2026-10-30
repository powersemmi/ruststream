//! The shared delivery body on embassy's executor, its timers on embassy-time's mock clock.
//!
//! A subscription task runs `run_sequential` over the channel broker; a driver task publishes,
//! moves the clock and reads what the body did. The executor runs until the driver's last check
//! passes; a failed check panics out of it and fails the binary.

// A thread-mode embassy task is never sent anywhere, and these futures hold the broker's `!Send`
// subscriber on purpose: that is what the body is held to here.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::alloc::System;
use std::sync::Arc;

use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream_embassy_probe::frames::{
    Frame, FrameDelivery, FrameSubscriber, Frames, Settlements,
};
use ruststream_embassy_probe::{CountDetached, EmbassyClock, Stop};
use ruststream_runtime::{
    Clock, Context, Delivery, DispatchFailure, FailurePolicies, Handler, HandlerOutcome,
    run_sequential,
};
use static_cell::StaticCell;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

// Counts every allocation of the process, so the driver reads how many one delivery made.
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// How long the handler waits on the clock before it finishes.
const HANDLER_WAIT: Duration = Duration::from_secs(10);

/// Executor rounds the driver yields after each step. A round polls every task that was woken
/// before it began, and the longest chain a step starts (the timer wakes the subscription task,
/// which finishes the handler, settles and parks again) takes two; the rest is margin.
const SETTLE_ROUNDS: usize = 8;

/// What a device keeps in `static`s: the broker's channel, the shutdown, the runner's host, and
/// what the test reads.
struct Probe {
    frames: Frames,
    settled: Settlements,
    stop: Stop,
    host: Arc<CountDetached>,
    handled: AtomicU32,
    ended: Cell<bool>,
    checked: Cell<bool>,
}

/// A handler that waits on the runner's clock, then counts the delivery as handled.
#[derive(Debug)]
struct WaitOnClock {
    handled: &'static AtomicU32,
}

impl Handler<FrameDelivery> for WaitOnClock {
    fn handle(
        &self,
        _delivery: &FrameDelivery,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        let handled = self.handled;
        async move {
            EmbassyClock.sleep(HANDLER_WAIT).await;
            handled.store(handled.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
            HandlerOutcome::ack()
        }
    }
}

/// The subscription's task: concrete, as every embassy task is, over the generic body.
#[embassy_executor::task]
async fn subscription(subscriber: FrameSubscriber, probe: &'static Probe) {
    let handler = Arc::new(WaitOnClock {
        handled: &probe.handled,
    });
    let delivery = Arc::new(Delivery::<()>::unscoped(probe.host.clone()));
    let failure = DispatchFailure::new(FailurePolicies::default(), &probe.stop);
    run_sequential(
        subscriber,
        handler,
        &probe.stop,
        Arc::from("readings"),
        Arc::new(()),
        delivery,
        failure,
    )
    .await;
    probe.ended.set(true);
}

/// Lets every task the last step woke run until it parks again.
async fn settle() {
    for _ in 0..SETTLE_ROUNDS {
        yield_now().await;
    }
}

/// Moves the mock clock and lets the woken tasks run.
async fn advance(by: Duration) {
    MockDriver::get().advance(Ticks::try_from(by).expect("the step fits the clock"));
    settle().await;
}

/// Reports one passed check the way the test harness does.
fn passed(check: &str) {
    println!("test {check} ... ok");
}

/// Publishes, moves the clock, reads what the body did, and lets the executor return.
///
/// Everything between the publish and the ack is read before the first report, so the count of
/// allocations covers the delivery path alone and not the printing.
#[embassy_executor::task]
async fn drive(probe: &'static Probe) {
    // The subscription starts first: what it allocates before its first turn is cold.
    let startup = Region::new(ALLOCATOR);
    settle().await;
    let cold = startup.change().allocations;
    let delivery_path = Region::new(ALLOCATOR);
    let frame = Frame::new(b"reading").expect("the payload fits a frame");
    probe.frames.send(frame).await;
    settle().await;
    let taken = probe.frames.is_empty();
    let early = probe.handled.load(Ordering::Relaxed);

    advance(Duration::from_secs(9)).await;
    let at_nine = (probe.handled.load(Ordering::Relaxed), probe.settled.acked());

    advance(Duration::from_secs(1)).await;
    let at_ten = (
        probe.handled.load(Ordering::Relaxed),
        probe.settled.acked(),
        probe.settled.nacked(),
    );
    let allocations = delivery_path.change().allocations;

    println!("cold allocations of the subscription task: {cold}");
    assert!(taken, "the body takes the delivery");
    assert_eq!(early, 0, "the handler waits on the clock");
    passed("the body takes the delivery and the handler waits");

    assert_eq!(at_nine, (0, 0), "handled or acked before the timer is due");
    passed("nine seconds in, the handler is still waiting");

    assert_eq!(
        at_ten,
        (1, 1, 0),
        "handled, acked, nacked once the timer is due"
    );
    passed("ten seconds in, the delivery is handled and acked");

    assert_eq!(allocations, 0, "allocations on the delivery path");
    passed("the delivery path allocates nothing");

    probe.stop.stop();
    settle().await;
    assert!(probe.ended.get(), "the loop outlives shutdown");
    assert!(!probe.stop.failed(), "the loop reported a failure");
    assert!(
        !probe.stop.stream_ended(),
        "the loop reported its stream ended"
    );
    assert_eq!(probe.host.detached(), 0, "work detached by the delivery");
    passed("shutdown ends the loop, with nothing failed or detached");

    probe.checked.set(true);
}

fn main() {
    static PROBE: StaticCell<Probe> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let probe: &'static Probe = PROBE.init(Probe {
        frames: Frames::new(),
        settled: Settlements::new(),
        stop: Stop::new(),
        host: Arc::new(CountDetached::new()),
        handled: AtomicU32::new(0),
        ended: Cell::new(false),
        checked: Cell::new(false),
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            let subscriber = FrameSubscriber::new(&probe.frames, &probe.settled);
            spawner.spawn(subscription(subscriber, probe).expect("the subscription task is free"));
            spawner.spawn(drive(probe).expect("the driver task is free"));
        },
        || probe.checked.get(),
    );
}
