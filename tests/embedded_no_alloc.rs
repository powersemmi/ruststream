//! A device service's deliveries driven through the embedded `TestApp` with an allocator that
//! fails the framework's own allocations. Built where the framework's `alloc` feature is off,
//! the one place the claim "no heap" is made; the harness's bookkeeping runs in a section the
//! allocator lets through.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::cell::Cell;
use std::error::Error;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::Headers;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER,
    SubscriberSettings,
};
use ruststream::runtime::{
    MessageWire, ReplyShape, Serialized as WrittenInPlace, SerializedReply, SerializedWire,
    WireBytes,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{BufferFull, Deserialized, EncodeBuf, Outgoing, Serialized, subscriber};

/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "jobs")]
struct Sent(&'static [u8]);

/// Asks for a retry later on a first delivery, and acknowledges the copy that carries its retry
/// count.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some("1") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry_after(LATER),
    }
}

/// The board's bus is a `static`: this test's own.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[test]
fn a_delivery_and_its_retry_make_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new(&BUS))).await?;
        tb.broker::<Bus>().message(&Sent(b"job")).publish().await?;
        tb.advance(LATER).await?;

        tb.broker::<Bus>().subscriber("jobs").assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}

/// A job that carries the tag the sensor put in its headers.
#[subscriber(LocalMemorySource::new("tagged"))]
async fn on_tagged(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str("sensor") {
        Some("boiler") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry(),
    }
}

/// A job from a sensor, to the destination of tagged jobs.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "tagged")]
struct Tagged(&'static [u8]);

/// A question for the board.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "ask")]
struct Ask(&'static [u8]);

/// The answer to an ask, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(&'static [u8]);

/// Answers every ask.
#[subscriber(LocalMemorySource::new("ask"), reply)]
async fn on_ask(ask: &Job<'_>) -> Answer {
    let _ = ask.0;
    Answer(b"yes")
}

/// Takes the answers.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answer(answer: &Job<'_>) -> HandlerOutcome {
    let _ = answer.0;
    HandlerOutcome::ack()
}

/// Holds its delivery for an hour of the clock, so the answers subscription of one message stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_stuck(held: &Job<'_>) -> HandlerOutcome {
    let _ = held.0;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// Answers an ask to a bus that is full, and counts the answers the bus refused.
#[subscriber(LocalMemorySource::new("pressed"), reply)]
async fn on_pressed(ask: &Job<'_>, ctx: &mut Context<'_, (), Counter>) -> Answer {
    let _ = ask.0;
    let refused = ctx.state().clone();
    if let Ok(after) = ctx.after(HandlerOutcome::ack()) {
        after.error(async move |failures: &Failures| {
            refused
                .0
                .set(refused.0.get() + u32::try_from(failures.len()).unwrap_or(u32::MAX));
        });
    }
    Answer(b"yes")
}

/// Counts what a continuation was told.
#[derive(Clone, Default)]
struct Counter(Rc<Cell<u32>>);

/// A subscription of one message, so the answers can fill it.
type Tight = LocalMemoryBroker<1>;

fn tagged_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_tagged))
}

fn reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_answer)
            .include(on_ask)
            .out_reply(LocalMemoryPublish)
    })
}

#[test]
fn a_delivery_with_headers_makes_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(tagged_app(Bus::new(&BUS))).await?;
        let mut headers = Headers::new();
        headers.insert("sensor", "boiler")?;
        tb.broker::<Bus>()
            .message(&Tagged(b"job"))
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("tagged")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_reply_makes_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(reply_app(Bus::new(&BUS))).await?;
        tb.broker::<Bus>().message(&Ask(b"?")).publish().await?;

        tb.broker::<Bus>()
            .subscriber("answers")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

/// A bus of one-message subscriptions, whose static is this test's own.
static TIGHT_BUS: LocalMemoryBus<1> = LocalMemoryBus::new();

fn pressed_app(bus: Tight) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| {
            b.include(on_stuck)
                .include(on_pressed)
                .out_reply(LocalMemoryPublish)
        })
}

/// A question for the pressed handler.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "pressed")]
struct Pressed(&'static [u8]);

#[test]
fn a_refused_reply_reaches_the_error_continuation_without_allocation() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let refused = Counter::default();
        let shared = refused.clone();
        let tb = TestApp::with_state(pressed_app(Tight::new(&TIGHT_BUS)), move |_| shared).await?;
        // The first answer fills the one slot of the answers subscription for good.
        tb.broker::<Tight>()
            .message(&Answer(b"x"))
            .publish()
            .await?;
        tb.broker::<Tight>()
            .message(&Pressed(b"?"))
            .publish()
            .await?;
        tb.advance(Duration::from_secs(1)).await?;

        assert_eq!(refused.0.get(), 1);
        tb.shutdown().await?;
        Ok(())
    })
}

/// Takes jobs on a pool.
#[subscriber(LocalMemorySource::new("pooled"))]
async fn on_pooled(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

/// Takes jobs on keyed lanes.
#[subscriber(LocalMemorySource::new("lanes"))]
async fn on_lane(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

/// Takes the jobs that arrived together.
#[subscriber(LocalMemorySource::new("batched"))]
async fn on_batched(jobs: &[Job<'_>]) -> HandlerOutcome {
    let _ = jobs.len();
    HandlerOutcome::ack()
}

/// A job for the pool.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "pooled")]
struct Pooled(&'static [u8]);

/// A job for the lanes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "lanes")]
struct Laned(&'static [u8]);

/// A job for the batch.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "batched")]
struct Batched(&'static [u8]);

fn lanes_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_pooled)
            .pool::<2>()
            .include(on_lane)
            .keyed::<2>()
    })
}

fn batch_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_batched.batch::<3>()))
}

#[test]
fn a_pool_and_keyed_lanes_make_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(lanes_app(Bus::new(&BUS))).await?;
        for _ in 0..3 {
            tb.broker::<Bus>()
                .message(&Pooled(b"job"))
                .publish()
                .await?;
            tb.broker::<Bus>().message(&Laned(b"job")).publish().await?;
        }

        tb.broker::<Bus>().subscriber("pooled").assert_called(3);
        tb.broker::<Bus>().subscriber("lanes").assert_called(3);
        tb.shutdown().await?;
        Ok(())
    })
}

/// The stack the batch scenario is held to: the default of a test thread on x86-64. A batch keeps
/// its deliveries and its outcomes in fixed storage, and the build without optimisation lays every
/// frame of the settling out in full, so a batch that grows its frames past this fails here.
/// Unoptimised frames on aarch64 are about twice as large, so the scenario gets twice the room
/// there; the device build's own size is held by `embedded_future_size`.
const BATCH_STACK: usize = if cfg!(target_arch = "x86_64") { 2 } else { 4 } * 1024 * 1024;

#[test]
fn a_batch_makes_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    let scenario = thread::Builder::new().stack_size(BATCH_STACK).spawn(|| {
        block_on(async {
            let scenario = async {
                let tb = TestApp::start(batch_app(Bus::new(&BUS))).await?;
                for _ in 0..3 {
                    tb.broker::<Bus>()
                        .message(&Batched(b"job"))
                        .publish()
                        .await?;
                }

                tb.broker::<Bus>().subscriber("batched").assert_called(3);
                tb.shutdown().await?;
                Ok::<(), Box<dyn Error>>(())
            };
            scenario.await.map_err(|err| err.to_string())
        })
    })?;
    scenario
        .join()
        .map_err(|_| "the batch scenario panicked")??;
    Ok(())
}

/// A bus whose messages carry up to 512 bytes.
type Wide = LocalMemoryBroker<4, 512>;

/// The wide board's bus, this test's own.
static WIDE_BUS: LocalMemoryBus<4, 512> = LocalMemoryBus::new();

/// Four hundred bytes of readings, written where the reply will stay.
#[derive(Outgoing)]
#[outgoing(name = "readings")]
struct Large;

impl WrittenInPlace for Large {
    type Error = BufferFull;

    fn wire_bytes(&self, buf: &mut EncodeBuf) -> Result<WireBytes<'_>, BufferFull> {
        buf.extend_from_slice(&[7; 400])?;
        Ok(WireBytes::InBuffer)
    }
}

impl MessageWire for Large {
    type Wire = SerializedWire;
}

impl ReplyShape for Large {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

/// A question for the wide board.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "poll")]
struct Poll(&'static [u8]);

/// Answers every poll with the readings.
#[subscriber(LocalMemorySource::new("poll"), reply)]
async fn on_poll(poll: &Job<'_>) -> Large {
    let _ = poll.0;
    Large
}

/// Takes the readings.
#[subscriber(LocalMemorySource::new("readings"))]
async fn on_readings(readings: &Job<'_>) -> HandlerOutcome {
    let _ = readings.0;
    HandlerOutcome::ack()
}

fn roomy_app(bus: Wide) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_readings)
            .include(on_poll)
            .out_reply(LocalMemoryPublish)
            .encode_capacity::<512>()
    })
}

fn default_app(bus: Wide) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_readings)
            .include(on_poll)
            .out_reply(LocalMemoryPublish)
    })
}

#[test]
fn a_mount_with_room_for_four_hundred_bytes_encodes_a_reply_of_that_size()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(roomy_app(Wide::new(&WIDE_BUS))).await?;
        tb.broker::<Wide>().message(&Poll(b"?")).publish().await?;

        tb.broker::<Wide>()
            .subscriber("readings")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_mount_at_the_default_room_refuses_a_reply_of_four_hundred_bytes() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let tb = TestApp::start(default_app(Wide::new(&WIDE_BUS))).await?;
        tb.broker::<Wide>().message(&Poll(b"?")).publish().await?;

        tb.broker::<Wide>().subscriber("readings").assert_called(0);
        tb.shutdown().await?;
        Ok(())
    })
}

/// Leaves a continuation that waits on the clock before it counts itself.
#[subscriber(LocalMemorySource::new("slow"))]
async fn on_slow(job: &Job<'_>, ctx: &mut Context<'_, (), Counter>) -> HandlerOutcome {
    let _ = job.0;
    let finished = ctx.state().clone();
    HandlerOutcome::ack().and_after(async move {
        embassy_time::Timer::after(embassy_time::Duration::from_secs(5)).await;
        finished.0.set(finished.0.get() + 1);
    })
}

/// A job for the slow continuation.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "slow")]
struct Slow(&'static [u8]);

fn sequential_slow_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| b.include(on_slow))
}

fn sized_slow_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| b.include(on_slow).encode_capacity::<512>())
}

fn pooled_slow_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| b.include(on_slow).pool::<2>())
}

fn keyed_slow_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| b.include(on_slow).keyed::<2>())
}

/// Publishes one job to an app whose handler leaves a five-second continuation, moves the clock
/// past it and returns how many continuations had finished by then.
async fn continuations_finished<Plan: Mounts<State = Counter> + 'static>(
    make: fn(Bus) -> EmbeddedApp<Plan>,
) -> Result<u32, Box<dyn Error>> {
    let finished = Counter::default();
    let shared = finished.clone();
    let tb = TestApp::with_state(make(Bus::new(&BUS)), move |_| shared).await?;
    tb.broker::<Bus>().message(&Slow(b"job")).publish().await?;
    tb.advance(Duration::from_secs(6)).await?;
    let count = finished.0.get();
    tb.shutdown().await?;
    Ok(count)
}

#[test]
fn a_continuation_on_one_lane_finishes_once_the_clock_reaches_it() -> Result<(), Box<dyn Error>> {
    block_on(async {
        assert_eq!(continuations_finished(sequential_slow_app).await?, 1);
        Ok(())
    })
}

#[test]
fn a_continuation_on_a_sized_lane_finishes_once_the_clock_reaches_it() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        assert_eq!(continuations_finished(sized_slow_app).await?, 1);
        Ok(())
    })
}

#[test]
fn a_continuation_on_a_pool_finishes_once_the_clock_reaches_it() -> Result<(), Box<dyn Error>> {
    block_on(async {
        assert_eq!(continuations_finished(pooled_slow_app).await?, 1);
        Ok(())
    })
}

#[test]
fn a_continuation_on_keyed_lanes_finishes_once_the_clock_reaches_it() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        assert_eq!(continuations_finished(keyed_slow_app).await?, 1);
        Ok(())
    })
}
