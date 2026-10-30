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
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;
use std::alloc::System;
use std::sync::Arc;

use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use ruststream_core::LocalBroker;
use ruststream_core::LocalIncomingMessage;
use ruststream_core::{BytesMut, CallerName, OutgoingDestination};
use ruststream_runtime::{
    Clock, Context, Delivery, DispatchFailure, FailurePolicies, HandlerOutcome, HeadersAtOnce,
    INSTRUMENTED_BODY, LocalDetach, LocalHandler, run_sequential,
};
use ruststream_runtime::{
    Deserialized, Handle, Input, LocalBlanketLayer, OpenRoutes, ReplyShape, Router, RunRoutes,
    Serialized, SerializedReply, SoloDeserialized, SubscriberSettings, WireBytes, subscriber,
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

/// The batch size of the app's batch subscription.
const BURST: NonZeroUsize = NonZeroUsize::new(4).expect("four is not zero");

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
    layered: AtomicU32,
    /// The burst channel its batch subscription reads.
    bursts: Frames,
    /// The batches its batch handler took, and the readings in them.
    batches: AtomicU32,
    batched: AtomicU32,
    /// The request channel its reply subscription reads, and the channel its replies go to.
    requests: Frames,
    replies: Frames,
    /// The app's host: counts the work its deliveries leave behind.
    host: LocalCount,
    ended: Cell<bool>,
}

/// A one-thread runner's host as the app task keeps it: counts what it is handed, which this
/// app's deliveries leave none of.
#[derive(Default)]
struct LocalCount(Cell<usize>);

impl LocalDetach for LocalCount {
    fn detach<Work>(&self, _work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        self.0.set(self.0.get() + 1);
    }

    fn detach_after<Work>(&self, _delay: Duration, _work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        self.0.set(self.0.get() + 1);
    }
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

/// A router-scope middleware a device service writes: counts every delivery that reaches the
/// router's handlers, before they run.
#[derive(Clone, Copy)]
struct CountDeliveries {
    reached: &'static AtomicU32,
}

/// The handler [`CountDeliveries`] wraps around each of the router's handlers.
struct Counted<H> {
    inner: H,
    reached: &'static AtomicU32,
}

impl LocalBlanketLayer for CountDeliveries {
    fn apply<M, C, S, H>(&self, handler: H) -> impl LocalHandler<M, C, S>
    where
        H: LocalHandler<M, C, S>,
    {
        Counted {
            inner: handler,
            reached: self.reached,
        }
    }
}

impl<M, C, S, H: LocalHandler<M, C, S>> LocalHandler<M, C, S> for Counted<H> {
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = HandlerOutcome> {
        self.reached.fetch_add(1, Ordering::Relaxed);
        self.inner.handle(msg, ctx)
    }
}

/// The app's batch handler: counts the batches it took and the readings in them.
struct CountBursts {
    batches: &'static AtomicU32,
    readings: &'static AtomicU32,
}

impl Handle<[Reading<'_>]> for CountBursts {
    fn handle(
        &self,
        readings: &[Reading<'_>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> + Send {
        self.batches.fetch_add(1, Ordering::Relaxed);
        let count = u32::try_from(readings.len()).expect("a batch fits the counter");
        self.readings.fetch_add(count, Ordering::Relaxed);
        ready(Ok(()))
    }
}

/// The app's answer to a request: its own bytes, lent to the publisher as they are.
struct Pong;

impl Serialized for Pong {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(b"pong"))
    }
}

// What `#[derive(Outgoing)]` writes: the mount site names where the reply goes.
impl OutgoingDestination for Pong {
    type Form = CallerName;
}

// The hand-written half of `#[derive(Serialized)]`: the reply leaves as its own bytes.
impl ReplyShape for Pong {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

/// The app's request handler: answers every request with a [`Pong`].
struct Answer;

impl Handle<Reading<'_>, Pong> for Answer {
    fn handle(
        &self,
        _request: &Reading<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Pong, HandlerOutcome>> + Send {
        ready(Ok(Pong))
    }
}

/// The app's routes, as a device service declares them.
fn routes(app: &'static App) -> impl OpenRoutes<FrameBroker> {
    let bursts = CountBursts {
        batches: &app.batches,
        readings: &app.batched,
    };
    Router::<FrameBroker, _, _, _, _>::local()
        .layer(CountDeliveries {
            reached: &app.layered,
        })
        .include(
            subscriber(
                FrameSource::new("readings"),
                CountReadings { seen: &app.seen },
            )
            .build(),
        )
        .include(
            subscriber(FrameSource::bursts("bursts"), bursts)
                .batch(BURST)
                .build(),
        )
        .include(
            subscriber(FrameSource::requests("requests"), Answer)
                .reply()
                .to("pongs")
                .build(),
        )
        .build()
}

/// The app's task: connects the broker, opens every route the router holds and runs them in one
/// future until the app's control stops it.
#[embassy_executor::task]
async fn app(broker: FrameBroker, app: &'static App) {
    let connected = broker.connect().await.expect("the broker connects");
    let opened = routes(app)
        .open(&connected)
        .await
        .expect("every route opens");
    // The task's future is its static storage: embassy keeps it in place, so it is not boxed.
    #[allow(clippy::large_futures)]
    opened.run(&app.stop, &Arc::new(()), &app.host).await;
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
    let layered = app.layered.load(Ordering::Relaxed);

    assert_eq!(settled, (1, 1, 1), "seen, acked, nacked through the router");
    passed("a router mounts a definition on a broker that is not Send");
    assert_eq!(layered, 2, "deliveries the router's layer saw");
    passed("the router's layer wraps its handlers");

    check_the_bursts(app).await;
    check_the_replies(app).await;
    assert_eq!(allocations, 0, "allocations on the router's delivery path");
    passed("the router's delivery path allocates nothing");

    assert_eq!(app.host.0.get(), 0, "work the app's deliveries detached");
    app.stop.stop();
    settle().await;
    assert!(app.ended.get(), "the app's run outlives shutdown");
    passed("shutdown ends the app's run");
}

/// Publishes three frames to the app's burst channel at once, and reads the batch its batch
/// subscription took.
async fn check_the_bursts(app: &App) {
    let acked = app.settled.acked();
    let first = burst(app).await;
    let taken = (
        app.batches.load(Ordering::Relaxed),
        app.batched.load(Ordering::Relaxed),
        app.settled.acked() - acked,
    );
    let second = burst(app).await;
    println!("allocations of the first batch: {first}, of the second: {second}");

    assert_eq!(taken, (1, 3, 3), "batches, readings, acks of one burst");
    passed("a batch subscription takes a burst as one batch and acks it");
    // The form's own: the batch's deliveries collected, and the vector of values built from
    // them, as on the multi-thread runner. The second batch is the steady state: what the first
    // initialized once is not on it.
    if INSTRUMENTED_BODY {
        // The harness's and OpenTelemetry's records ride every batch of such a build.
        println!(
            "skipped: the body of this build records batches for the harness or OpenTelemetry"
        );
    } else {
        assert_eq!(second, BATCH_ALLOCATIONS, "allocations of one batch");
        passed("a batch allocates what the form allocates on any runner");
    }
}

/// Publishes three frames to the app's burst channel at once and lets the app take them,
/// counting what that allocated.
async fn burst(app: &App) -> usize {
    let burst = Region::new(ALLOCATOR);
    for payload in [b"one".as_slice(), b"two", b"three"] {
        app.bursts
            .send(Frame::new(payload).expect("the payload fits a frame"))
            .await;
    }
    settle().await;
    burst.change().allocations
}

/// Sends a request to the app's reply subscription, and reads the reply it published.
async fn check_the_replies(app: &App) {
    let acked = app.settled.acked();
    let replying = Region::new(ALLOCATOR);
    app.requests
        .send(Frame::new(b"ping").expect("the payload fits a frame"))
        .await;
    settle().await;
    let allocations = replying.change().allocations;
    let reply = app.replies.try_receive().ok();

    assert_eq!(
        reply.as_ref().map(Frame::payload),
        Some(b"pong".as_slice()),
        "the reply on the reply channel"
    );
    assert_eq!(app.settled.acked() - acked, 1, "the request is acked");
    passed("a reply definition publishes its reply and acks the request");
    assert_eq!(allocations, 0, "allocations of one request and its reply");
    passed("a reply through a lending publisher allocates nothing");
}

/// What one batch of the self-deserializing form allocates on any runner: the deliveries it
/// collects and the values it builds from them.
const BATCH_ALLOCATIONS: usize = 2;

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
            layered: AtomicU32::new(0),
            bursts: Frames::new(),
            batches: AtomicU32::new(0),
            batched: AtomicU32::new(0),
            requests: Frames::new(),
            replies: Frames::new(),
            host: LocalCount::default(),
            ended: Cell::new(false),
        },
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            let subscriber = FrameSubscriber::new(&probe.frames, &probe.settled);
            spawner.spawn(subscription(subscriber, probe).expect("the subscription task is free"));
            let broker = FrameBroker::new(&probe.app.frames, &probe.app.settled)
                .with_bursts(&probe.app.bursts)
                .with_requests(&probe.app.requests)
                .with_replies(&probe.app.replies);
            spawner.spawn(app(broker, &probe.app).expect("the app task is free"));
            spawner.spawn(drive(probe).expect("the driver task is free"));
        },
        || probe.checked.get(),
    );
}
