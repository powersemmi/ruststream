//! The shared delivery body on embassy's executor, its timers on embassy-time's mock clock.
//!
//! A subscription task runs `run_sequential` over the channel broker; a driver task publishes,
//! moves the clock and reads what the body did. The executor runs until the driver's last check
//! passes; a failed check panics out of it and fails the binary.

// A thread-mode embassy task is never sent anywhere, and these futures hold the broker's `!Send`
// subscriber on purpose: that is what the body is held to here.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::convert::Infallible;
use core::future::ready;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::alloc::System;
use std::sync::Arc;

use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream_core::LocalBroker;
use ruststream_core::LocalIncomingMessage;
use ruststream_runtime::{
    Clock, Context, Delivery, DispatchFailure, FailurePolicies, HandlerOutcome, HeadersAtOnce,
    LocalHandler, run_sequential,
};
use ruststream_runtime::{
    Deserialized, Handle, Input, OpenRoutes, Router, RunRoutes, SoloDeserialized, subscriber,
};
use ruststream_runtime_embassy::frames::{
    Frame, FrameBroker, FrameDelivery, FrameSource, FrameSubscriber, Frames, Settlements,
};
use ruststream_runtime_embassy::{Control, CountDetached, EmbassyClock, Health};
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
    stop: Control,
    host: Arc<CountDetached>,
    handled: Cell<u32>,
    ended: Cell<bool>,
    checked: Cell<bool>,
    app: App,
}

/// The second broker's statics: an app whose subscription a `Router` mounts.
struct App {
    frames: Frames,
    settled: Settlements,
    stop: Control,
    seen: AtomicU32,
    ended: Cell<bool>,
}

/// A reading as the device's handler takes it: a view over the frame's bytes, decoded by no
/// codec.
struct Reading<'a>(&'a [u8]);

impl Deserialized for Reading<'_> {
    type Output<'a> = Reading<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> {
        Ok(Reading(payload))
    }
}

impl Input for Reading<'_> {
    type Axis = SoloDeserialized<Reading<'static>>;
}

/// The app's handler: counts the readings it saw, and drops an empty one.
struct CountReadings {
    seen: &'static AtomicU32,
}

impl Handle<Reading<'_>> for CountReadings {
    fn handle(
        &self,
        reading: &Reading<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> + Send {
        let verdict = if reading.0.is_empty() {
            Err(HandlerOutcome::drop())
        } else {
            self.seen.fetch_add(1, Ordering::Relaxed);
            Ok(())
        };
        ready(verdict)
    }
}

/// The app's routes, as a device service declares them.
fn routes(seen: &'static AtomicU32) -> impl OpenRoutes<FrameBroker> {
    Router::<FrameBroker>::new()
        .include(subscriber(FrameSource::new("readings"), CountReadings { seen }).build())
}

/// The app's task: connects the broker, opens every route the router holds and runs them in one
/// future until the app's control stops it.
#[embassy_executor::task]
async fn app(broker: FrameBroker, app: &'static App, host: &'static CountDetached) {
    let connected = broker.connect().await.expect("the broker connects");
    let opened = routes(&app.seen)
        .open(&connected)
        .await
        .expect("every route opens");
    opened.run(&app.stop, &Arc::new(()), host).await;
    app.ended.set(true);
}

/// The payload a device's handler cannot use: it drops the frame at once.
const SPOILED: &[u8] = b"spoiled";

/// A handler that drops a [`SPOILED`] frame, and waits on the runner's clock for any other before
/// it counts the delivery as handled. It holds the delivery and a cell across the wait, so its
/// future is not `Send`.
#[derive(Debug)]
struct WaitOnClock {
    handled: &'static Cell<u32>,
}

impl LocalHandler<FrameDelivery> for WaitOnClock {
    async fn handle(&self, delivery: &FrameDelivery, _ctx: &mut Context<'_>) -> HandlerOutcome {
        if delivery.payload() == SPOILED {
            return HandlerOutcome::drop();
        }
        EmbassyClock.sleep(HANDLER_WAIT).await;
        if delivery.payload().is_empty() {
            return HandlerOutcome::drop();
        }
        self.handled.set(self.handled.get() + 1);
        HandlerOutcome::ack()
    }
}

/// The subscription's task: concrete, as every embassy task is, over the generic body.
#[embassy_executor::task]
async fn subscription(subscriber: FrameSubscriber, probe: &'static Probe) {
    let handler = WaitOnClock {
        handled: &probe.handled,
    };
    let delivery = Delivery::<()>::unscoped();
    let failure = DispatchFailure::new(FailurePolicies::default(), &probe.stop);
    run_sequential(
        subscriber,
        handler,
        &probe.stop,
        Arc::from("readings"),
        Arc::new(()),
        (&delivery, &*probe.host),
        failure,
        (),
        HeadersAtOnce,
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
    let early = probe.handled.get();

    advance(Duration::from_secs(9)).await;
    let at_nine = (probe.handled.get(), probe.settled.acked());

    advance(Duration::from_secs(1)).await;
    let at_ten = (
        probe.handled.get(),
        probe.settled.acked(),
        probe.settled.nacked(),
    );

    let spoiled = Frame::new(SPOILED).expect("the payload fits a frame");
    probe.frames.send(spoiled).await;
    settle().await;
    let dropped = (
        probe.handled.get(),
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

    assert_eq!(
        dropped,
        (1, 1, 1),
        "handled, acked, nacked once the handler dropped a frame"
    );
    passed("a dropped frame is nacked at once, without the clock");

    assert_eq!(allocations, 0, "allocations on the delivery path");
    passed("the delivery path allocates nothing");

    probe.stop.stop();
    settle().await;
    assert!(probe.ended.get(), "the loop outlives shutdown");
    assert_ne!(
        probe.stop.health(),
        Health::Failed,
        "the loop reported a failure"
    );
    assert!(
        !probe.stop.stream_ended(),
        "the loop reported its stream ended"
    );
    assert_eq!(probe.host.detached(), 0, "work detached by the delivery");
    passed("shutdown ends the loop, with nothing failed or detached");

    check_the_app(&probe.app).await;
    probe.checked.set(true);
}

/// Publishes to the app a `Router` mounted, and reads what its subscription did.
async fn check_the_app(app: &App) {
    let delivery_path = Region::new(ALLOCATOR);
    app.frames
        .send(Frame::new(b"reading").expect("the payload fits a frame"))
        .await;
    settle().await;
    app.frames
        .send(Frame::new(b"").expect("the payload fits a frame"))
        .await;
    settle().await;
    let allocations = delivery_path.change().allocations;
    let settled = (
        app.seen.load(Ordering::Relaxed),
        app.settled.acked(),
        app.settled.nacked(),
    );

    assert_eq!(settled, (1, 1, 1), "seen, acked, nacked through the router");
    passed("a router mounts a definition on a broker that is not Send");
    assert_eq!(allocations, 0, "allocations on the router's delivery path");
    passed("the router's delivery path allocates nothing");

    app.stop.stop();
    settle().await;
    assert!(app.ended.get(), "the app's run outlives shutdown");
    passed("shutdown ends the app's run");
}

fn main() {
    static PROBE: StaticCell<Probe> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let probe: &'static Probe = PROBE.init(Probe {
        frames: Frames::new(),
        settled: Settlements::new(),
        stop: Control::new(),
        host: Arc::new(CountDetached::new()),
        handled: Cell::new(0),
        ended: Cell::new(false),
        checked: Cell::new(false),
        app: App {
            frames: Frames::new(),
            settled: Settlements::new(),
            stop: Control::new(),
            seen: AtomicU32::new(0),
            ended: Cell::new(false),
        },
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            let subscriber = FrameSubscriber::new(&probe.frames, &probe.settled);
            spawner.spawn(subscription(subscriber, probe).expect("the subscription task is free"));
            let broker = FrameBroker::new(&probe.app.frames, &probe.app.settled);
            spawner.spawn(app(broker, &probe.app, &probe.host).expect("the app task is free"));
            spawner.spawn(drive(probe).expect("the driver task is free"));
        },
        || probe.checked.get(),
    );
}
