//! The delivery body on embassy's executor, its timers on embassy-time's mock clock.
//!
//! A subscription task runs the body over the channel broker; a driver task publishes, moves the
//! clock and reads what the body did. The executor runs until the driver's last check passes; a
//! failed check panics out of it and fails the binary.

// A thread-mode embassy task is never sent anywhere, and these futures hold the broker's `!Send`
// types on purpose: that is what the body is held to here.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::future::poll_fn;
use core::pin::pin;
use core::time::Duration;

use embassy_executor::{Executor, Spawner};
use embassy_futures::select::{Either, select};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use futures_core::Stream;
use ruststream_core::{LocalIncomingMessage, LocalSubscriber};
use ruststream_embassy_probe::frames::{Frame, FrameSubscriber, Frames, Settlements};
use ruststream_embassy_probe::{EmbassyClock, Stop};
use ruststream_runtime::{Clock, ShutdownWait};
use static_cell::StaticCell;

/// How long the handler waits on the clock before it finishes.
const HANDLER_WAIT: Duration = Duration::from_secs(10);

/// Executor rounds the driver yields after each step. A round polls every task that was woken
/// before it began, and the longest chain a step starts (the timer wakes the subscription task,
/// which finishes the handler, settles and parks again) takes two; the rest is margin.
const SETTLE_ROUNDS: usize = 8;

/// What a device keeps in `static`s: the broker's channel, the shutdown, and what the test reads.
struct Probe {
    frames: Frames,
    settled: Settlements,
    stop: Stop,
    handled: Cell<u32>,
    ended: Cell<bool>,
    checked: Cell<bool>,
}

/// A loop over a subscription until shutdown: the stand-in for the shared body.
async fn serve<Sub, Shutdown, Timers, Handle>(
    mut subscriber: Sub,
    shutdown: &Shutdown,
    clock: &Timers,
    mut handler: Handle,
) where
    Sub: LocalSubscriber,
    Shutdown: ShutdownWait,
    Timers: Clock,
    Handle: AsyncFnMut(&[u8], &Timers),
{
    let mut deliveries = pin!(subscriber.stream());
    let mut stopping = pin!(shutdown.cancelled());
    while !shutdown.is_cancelled() {
        let next = poll_fn(|cx| deliveries.as_mut().poll_next(cx));
        match select(next, stopping.as_mut()).await {
            Either::First(Some(Ok(delivery))) => {
                handler(delivery.payload(), clock).await;
                // The stand-in broker's settlement cannot fail.
                let _ = LocalIncomingMessage::ack(delivery).await;
            }
            Either::First(Some(Err(_))) => {}
            Either::First(None) | Either::Second(()) => break,
        }
    }
}

/// The subscription's task: concrete, as every embassy task is, over the generic body.
#[embassy_executor::task]
async fn subscription(subscriber: FrameSubscriber, probe: &'static Probe) {
    serve(
        subscriber,
        &probe.stop,
        &EmbassyClock,
        async |_payload: &[u8], clock: &EmbassyClock| {
            clock.sleep(HANDLER_WAIT).await;
            probe.handled.set(probe.handled.get() + 1);
        },
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
#[embassy_executor::task]
async fn drive(probe: &'static Probe) {
    let frame = Frame::new(b"reading").expect("the payload fits a frame");
    probe.frames.send(frame).await;
    settle().await;
    assert!(probe.frames.is_empty(), "the body takes the delivery");
    assert_eq!(probe.handled.get(), 0, "the handler waits on the clock");
    passed("the body takes the delivery and the handler waits");

    advance(Duration::from_secs(9)).await;
    assert_eq!(probe.handled.get(), 0, "handled before the timer is due");
    assert_eq!(
        probe.settled.acked(),
        0,
        "settled before the handler finished"
    );
    passed("nine seconds in, the handler is still waiting");

    advance(Duration::from_secs(1)).await;
    assert_eq!(probe.handled.get(), 1, "handled once the timer is due");
    assert_eq!(probe.settled.acked(), 1, "acked after the handler");
    assert_eq!(probe.settled.nacked(), 0, "nacked a handled delivery");
    passed("ten seconds in, the delivery is handled and acked");

    probe.stop.stop();
    settle().await;
    assert!(probe.ended.get(), "the loop outlives shutdown");
    passed("shutdown ends the loop");

    probe.checked.set(true);
}

fn main() {
    static PROBE: StaticCell<Probe> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let probe: &'static Probe = PROBE.init(Probe {
        frames: Frames::new(),
        settled: Settlements::new(),
        stop: Stop::new(),
        handled: Cell::new(0),
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
