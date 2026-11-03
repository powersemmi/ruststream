//! A device service's deliveries driven through the embedded `TestApp` with an allocator that
//! fails the framework's own allocations. Built where the framework's `alloc` feature is off,
//! the one place the claim "no heap" is made; the harness's bookkeeping runs in a section the
//! allocator lets through.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::cell::Cell;
use std::error::Error;
use std::iter::once;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::Headers;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::nonzero;
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, Outcomes, RETRY_COUNT_HEADER,
    SubscriberSettings,
};
use ruststream::runtime::{
    MessageWire, ReplyShape, Serialized as WrittenInPlace, SerializedReply, SerializedWire,
    WireBytes,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{
    BufferFull, Deserialized, EncodeBuf, Outgoing, PayloadBuf, Serialized, subscriber,
};

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

/// Takes jobs on keyed workers.
#[subscriber(LocalMemorySource::new("workers"))]
async fn on_worker(job: &Job<'_>) -> HandlerOutcome {
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

/// A job for the workers.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "workers")]
struct KeyedJob(&'static [u8]);

/// A job for the batch.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "batched")]
struct Batched(&'static [u8]);

fn workers_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_pooled)
            .pool::<2>()
            .include(on_worker)
            .keyed::<2>()
    })
}

fn batch_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_batched.batch::<3>()))
}

#[test]
fn a_pool_and_keyed_workers_make_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(workers_app(Bus::new(&BUS))).await?;
        for _ in 0..3 {
            tb.broker::<Bus>()
                .message(&Pooled(b"job"))
                .publish()
                .await?;
            tb.broker::<Bus>()
                .message(&KeyedJob(b"job"))
                .publish()
                .await?;
        }

        tb.broker::<Bus>().subscriber("pooled").assert_called(3);
        tb.broker::<Bus>().subscriber("workers").assert_called(3);
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
fn a_continuation_on_one_worker_finishes_once_the_clock_reaches_it() -> Result<(), Box<dyn Error>> {
    block_on(async {
        assert_eq!(continuations_finished(sequential_slow_app).await?, 1);
        Ok(())
    })
}

#[test]
fn a_continuation_on_a_sized_worker_finishes_once_the_clock_reaches_it()
-> Result<(), Box<dyn Error>> {
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
fn a_continuation_on_keyed_workers_finishes_once_the_clock_reaches_it() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        assert_eq!(continuations_finished(keyed_slow_app).await?, 1);
        Ok(())
    })
}

/// A bus of one-message subscriptions whose messages carry up to 512 bytes.
type WideTight = LocalMemoryBroker<1, 512>;

/// The tight wide board's bus, this test's own.
static WIDE_TIGHT_BUS: LocalMemoryBus<1, 512> = LocalMemoryBus::new();

/// A reading, to the destination the stuck handler holds.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "readings")]
struct Reading(&'static [u8]);

/// Holds its delivery for an hour of the clock, so the readings subscription stays full.
#[subscriber(LocalMemorySource::new("readings"))]
async fn on_held(reading: &Job<'_>) -> HandlerOutcome {
    let _ = reading.0;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// Answers every poll with the readings, and counts the refused answers whose bytes it can no
/// longer read.
#[subscriber(LocalMemorySource::new("poll"), reply)]
async fn on_pressed_poll(poll: &Job<'_>, ctx: &mut Context<'_, (), Counter>) -> Large {
    let _ = poll.0;
    let unreadable = ctx.state().clone();
    if let Ok(after) = ctx.after(HandlerOutcome::ack()) {
        after.error(async move |failures: &Failures| {
            for failed in failures {
                if failed.message().wire_bytes(&mut PayloadBuf::new()).is_err() {
                    unreadable.0.set(unreadable.0.get() + 1);
                }
            }
        });
    }
    Large
}

fn pressed_poll_app(bus: WideTight) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| {
            b.include(on_held)
                .include(on_pressed_poll)
                .out_reply(LocalMemoryPublish)
                .encode_capacity::<512>()
        })
}

#[test]
fn a_refused_reply_too_large_to_keep_reaches_the_error_continuation_without_bytes()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let unreadable = Counter::default();
        let shared = unreadable.clone();
        let tb = TestApp::with_state(
            pressed_poll_app(WideTight::new(&WIDE_TIGHT_BUS)),
            move |_| shared,
        )
        .await?;
        tb.broker::<WideTight>()
            .message(&Reading(b"1"))
            .publish()
            .await?;
        tb.broker::<WideTight>()
            .message(&Poll(b"?"))
            .publish()
            .await?;
        tb.advance(Duration::from_secs(1)).await?;

        assert_eq!(unreadable.0.get(), 1, "failures that kept no bytes");
        tb.shutdown().await?;
        Ok(())
    })
}

/// The batch bus of the batch retry scenarios, this test's own: four jobs per subscription.
static BATCH_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// A job the batch scenarios publish, to the destination they name.
#[derive(Outgoing, Serialized)]
struct Posted(&'static [u8]);

/// The stack a batch scenario with a store of retry copies is held to: the store's records come
/// on top of the batch's frames, which the build without optimisation lays out in full.
const STORE_STACK: usize = 4 * BATCH_STACK;

/// Runs a batch scenario on a thread of the stack [`STORE_STACK`] gives it.
fn on_batch_stack(
    scenario: impl AsyncFnOnce() -> Result<(), Box<dyn Error>> + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .stack_size(STORE_STACK)
        .spawn(move || block_on(scenario()).map_err(|err| err.to_string()))?
        .join()
        .map_err(|_| "the batch scenario panicked")??;
    Ok(())
}

/// Holds the handler for a second of the clock on a `gate` job, so the jobs behind it make one
/// batch.
async fn hold_on_gate(jobs: &[Job<'_>]) {
    if jobs.iter().any(|job| job.0 == b"gate") {
        embassy_time::Timer::after(embassy_time::Duration::from_secs(1)).await;
    }
}

/// Asks one retry for the whole batch.
#[subscriber(LocalMemorySource::new("bwhole"))]
async fn on_whole(jobs: &[Job<'_>]) -> HandlerOutcome {
    hold_on_gate(jobs).await;
    if jobs.iter().any(|job| job.0 == b"gate") {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(LATER)
    }
}

/// Asks a retry for every job but `ok`, which it acknowledges.
#[subscriber(LocalMemorySource::new("beach"))]
async fn on_each(jobs: &[Job<'_>]) -> Outcomes<HandlerOutcome, 3> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| match job.0 {
            b"ok" | b"gate" => HandlerOutcome::ack(),
            _ => HandlerOutcome::retry_after(LATER),
        })
        .collect()
}

/// Asks a retry for every job but `ok`, and counts the copies it loses.
#[subscriber(LocalMemorySource::new("blossy"))]
async fn on_lossy(
    jobs: &[Job<'_>],
    ctx: &mut Context<'_, (), Counter>,
) -> Outcomes<HandlerOutcome, 3> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| match job.0 {
            b"ok" | b"gate" => HandlerOutcome::ack(),
            _ => {
                let lost = ctx.state().clone();
                HandlerOutcome::retry_after(LATER).error(async move |failures: &Failures| {
                    lost.0
                        .set(lost.0.get() + u32::try_from(failures.len()).unwrap_or(u32::MAX));
                })
            }
        })
        .collect()
}

/// Asks a retry for every job, one outcome each.
#[subscriber(LocalMemorySource::new("bdead"))]
async fn on_dead_jobs(jobs: &[Job<'_>]) -> Outcomes<HandlerOutcome, 3> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| match job.0 {
            b"gate" => HandlerOutcome::ack(),
            _ => HandlerOutcome::retry_after(LATER),
        })
        .collect()
}

/// Takes what the cap sent away.
#[subscriber(LocalMemorySource::new("bdeadletter"))]
async fn on_dead_letter(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

/// Acks the batch and leaves one continuation that counts it.
#[subscriber(LocalMemorySource::new("bthen"))]
async fn on_then(jobs: &[Job<'_>], ctx: &mut Context<'_, (), Counter>) -> HandlerOutcome {
    hold_on_gate(jobs).await;
    if jobs.iter().any(|job| job.0 == b"gate") {
        return HandlerOutcome::ack();
    }
    let ran = ctx.state().clone();
    HandlerOutcome::ack().and_after(async move { ran.0.set(ran.0.get() + 1) })
}

/// Acks every job and leaves a continuation for each that counts it after a second.
#[subscriber(LocalMemorySource::new("bthen.each"))]
async fn on_then_each(
    jobs: &[Job<'_>],
    ctx: &mut Context<'_, (), Counter>,
) -> Outcomes<HandlerOutcome, 3> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| {
            if job.0 == b"gate" {
                return HandlerOutcome::ack();
            }
            let ran = ctx.state().clone();
            HandlerOutcome::ack().and_after(async move {
                embassy_time::Timer::after(embassy_time::Duration::from_secs(1)).await;
                ran.0.set(ran.0.get() + 1);
            })
        })
        .collect()
}

/// An app of one batch mount, built by `mount`, with a counter in its state.
macro_rules! batch_app_of {
    ($name:ident, $($mount:tt)+) => {
        fn $name(bus: Bus) -> EmbeddedApp<impl Mounts<State = Counter>> {
            EmbeddedApp::new()
                .with_state(Counter::default())
                .with_broker(bus, |b| b.$($mount)+)
        }
    };
}

batch_app_of!(
    whole_app,
    include(on_whole.batch::<3>()).retry_capacity::<3>()
);
batch_app_of!(
    each_app,
    include(on_each.batch::<3>()).retry_capacity::<3>()
);
batch_app_of!(
    lossy_app,
    include(on_lossy.batch::<3>()).retry_capacity::<3>()
);
batch_app_of!(
    then_app,
    include(on_then.batch::<3>()).include(on_then_each.batch::<3>())
);

fn dead_letter_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Counter>> {
    EmbeddedApp::new()
        .with_state(Counter::default())
        .with_broker(bus, |b| {
            b.include(on_dead_letter)
                .include(on_dead_jobs.batch::<3>())
                .max_attempts(nonzero!(2u32))
                .dead_letter("bdeadletter")
                .retry_capacity::<3>()
        })
}

/// Publishes a gate to `to`, then `jobs` behind it, and lets the gate open: the jobs arrive as one
/// batch.
async fn publish_batch(
    tb: &TestApp,
    to: &str,
    jobs: [&'static [u8]; 3],
) -> Result<(), Box<dyn Error>> {
    for job in once(&b"gate"[..]).chain(jobs) {
        tb.broker::<Bus>()
            .message(&Posted(job))
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

/// Starts `app` with a counter the test keeps a handle to.
async fn start_batch_app<Built: Mounts<State = Counter> + 'static>(
    app: fn(Bus) -> EmbeddedApp<Built>,
) -> Result<(TestApp, Counter), Box<dyn Error>> {
    let counter = Counter::default();
    let shared = counter.clone();
    let tb = TestApp::with_state(app(Bus::new(&BATCH_BUS)), move |_| shared).await?;
    Ok((tb, counter))
}

#[test]
fn a_whole_batch_retry_makes_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>> {
    on_batch_stack(async || {
        let (tb, _counter) = start_batch_app(whole_app).await?;
        publish_batch(&tb, "bwhole", [b"a", b"b", b"c"]).await?;
        tb.advance(LATER).await?;

        tb.broker::<Bus>()
            .subscriber("bwhole")
            .assert_batch_sizes(&[1, 3, 3]);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn elements_with_mixed_outcomes_make_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>>
{
    on_batch_stack(async || {
        let (tb, _counter) = start_batch_app(each_app).await?;
        publish_batch(&tb, "beach", [b"a", b"ok", b"c"]).await?;
        tb.advance(LATER).await?;

        let batches = tb.broker::<Bus>().subscriber("beach").batches_raw();
        let copies: Vec<Vec<u8>> = batches
            .iter()
            .skip(2)
            .flatten()
            .map(|one| one.to_vec())
            .collect();
        assert_eq!(copies, [b"a".to_vec(), b"c".to_vec()]);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_full_store_reaches_the_error_continuation_of_a_batch_element_without_allocation()
-> Result<(), Box<dyn Error>> {
    on_batch_stack(async || {
        let (tb, lost) = start_batch_app(lossy_app).await?;
        publish_batch(&tb, "blossy", [b"a", b"b", b"c"]).await?;
        publish_batch(&tb, "blossy", [b"d", b"ok", b"e"]).await?;

        assert_eq!(lost.0.get(), 2);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn the_element_that_reaches_the_cap_goes_to_the_dead_letter_without_allocation()
-> Result<(), Box<dyn Error>> {
    on_batch_stack(async || {
        let (tb, _counter) = start_batch_app(dead_letter_app).await?;
        tb.broker::<Bus>()
            .message(&Posted(b"a"))
            .to("bdead")
            .publish()
            .await?;
        tb.advance(Duration::from_millis(4500)).await?;
        for job in [&b"gate"[..], &b"n"[..]] {
            tb.broker::<Bus>()
                .message(&Posted(job))
                .to("bdead")
                .publish()
                .await?;
        }
        tb.advance(Duration::from_millis(600)).await?;
        tb.advance(Duration::from_millis(500)).await?;

        let bus = tb.broker::<Bus>();
        bus.subscriber("bdead").assert_batch_sizes(&[1, 1, 2]);
        bus.subscriber("bdeadletter").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn the_continuations_of_a_batch_make_no_allocation_of_the_framework() -> Result<(), Box<dyn Error>>
{
    on_batch_stack(async || {
        let (tb, ran) = start_batch_app(then_app).await?;
        publish_batch(&tb, "bthen", [b"a", b"b", b"c"]).await?;
        assert_eq!(ran.0.get(), 1, "one continuation for the whole batch");

        publish_batch(&tb, "bthen.each", [b"a", b"b", b"c"]).await?;
        // The worker's one slot holds a continuation at a time, and the clock moves in the steps
        // the test takes: each continuation waits its second after the one before it.
        for _ in 0..3 {
            tb.advance(Duration::from_secs(1)).await?;
        }
        assert_eq!(ran.0.get(), 4, "and one for each element of the next");
        tb.shutdown().await?;
        Ok(())
    })
}

/// What the uniform outcomes of the fan scenarios were told: how many handler calls there were,
/// how many times an `.error` ran, the payload digits of the failures it read (in the order it
/// read them), and how many continuations followed.
#[derive(Clone, Default)]
struct Tally(Rc<TallyCells>);

#[derive(Default)]
struct TallyCells {
    calls: Cell<u32>,
    errors: Cell<u32>,
    lost: Cell<u32>,
    thens: Cell<u32>,
    acked: Cell<u32>,
}

/// How long the first call's copies hold their records: past every scenario below.
const FILL: Duration = Duration::from_secs(60);

/// The first call asks a retry for the batch and reads no failure; later calls ask the same with an
/// `.error` that reads every failed copy and a `.then`, except a lone `z`, which is acknowledged.
#[subscriber(LocalMemorySource::new("bfan"))]
async fn on_fan(jobs: &[Job<'_>], ctx: &mut Context<'_, (), Tally>) -> HandlerOutcome {
    hold_on_gate(jobs).await;
    if jobs.iter().any(|job| job.0 == b"gate") {
        return HandlerOutcome::ack();
    }
    // A copy that came back is acknowledged: the scenarios follow the first deliveries.
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let tally = ctx.state().clone();
    let call = tally.0.calls.get();
    tally.0.calls.set(call + 1);
    if call == 0 {
        return HandlerOutcome::retry_after(FILL);
    }
    if jobs.iter().any(|job| job.0 == b"z") {
        return HandlerOutcome::ack().and_after(async move {
            tally.0.acked.set(tally.0.acked.get() + 1);
        });
    }
    let after = tally.clone();
    HandlerOutcome::retry_after(LATER)
        .and_after(async move { after.0.thens.set(after.0.thens.get() + 1) })
        .error(async move |failures: &Failures| {
            tally.0.errors.set(tally.0.errors.get() + 1);
            for failed in failures {
                let digit = failed
                    .message()
                    .payload()
                    .and_then(|payload| payload.first())
                    .map_or(0, |byte| u32::from(byte - b'0'));
                tally.0.lost.set(tally.0.lost.get() * 10 + digit);
            }
        })
}

fn fan_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Tally>> {
    EmbeddedApp::new()
        .with_state(Tally::default())
        .with_broker(bus, |b| {
            b.include(on_fan.batch::<3>()).retry_capacity::<3>()
        })
}

/// Starts the fan app with a tally the test keeps a handle to.
async fn start_fan_app() -> Result<(TestApp, Tally), Box<dyn Error>> {
    let tally = Tally::default();
    let shared = tally.clone();
    let tb = TestApp::with_state(fan_app(Bus::new(&BATCH_BUS)), move |_| shared).await?;
    Ok((tb, tally))
}

/// Publishes a gate to `bfan`, then `jobs` behind it, and lets the gate open: the jobs arrive as
/// one batch.
async fn publish_jobs(tb: &TestApp, jobs: &[&'static [u8]]) -> Result<(), Box<dyn Error>> {
    for job in once(&b"gate"[..]).chain(jobs.iter().copied()) {
        tb.broker::<Bus>()
            .message(&Posted(job))
            .to("bfan")
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn a_uniform_error_reads_every_failed_copy_of_the_batch_once_after_the_last_copy_is_reported()
-> Result<(), Box<dyn Error>> {
    on_batch_stack(async || {
        let (tb, tally) = start_fan_app().await?;
        // Two records go to the first batch's copies, so of the next three jobs one waits for its
        // delay and two find no record.
        publish_jobs(&tb, &[b"1", b"2"]).await?;
        publish_jobs(&tb, &[b"3", b"4", b"5"]).await?;
        assert_eq!(
            tally.0.errors.get(),
            0,
            "the delayed copy has not been reported"
        );

        tb.advance(LATER).await?;
        assert_eq!(tally.0.errors.get(), 1, "one call for the batch");
        assert_eq!(
            tally.0.lost.get(),
            45,
            "both failures, in the order of the batch"
        );
        assert_eq!(
            tally.0.thens.get(),
            1,
            "a copy went out, so the follow-up runs"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_uniform_batch_where_every_copy_failed_calls_the_error_and_not_the_follow_up()
-> Result<(), Box<dyn Error>> {
    on_batch_stack(async || {
        let (tb, tally) = start_fan_app().await?;
        publish_jobs(&tb, &[b"1", b"2", b"3"]).await?;
        publish_jobs(&tb, &[b"4", b"5", b"6"]).await?;

        assert_eq!(tally.0.errors.get(), 1);
        assert_eq!(tally.0.lost.get(), 456);
        assert_eq!(tally.0.thens.get(), 0, "no copy went out");
        tb.shutdown().await?;
        Ok(())
    })
}
#[test]
fn a_second_uniform_error_waits_for_the_first_and_reads_only_its_own_failures()
-> Result<(), Box<dyn Error>> {
    on_batch_stack(async || {
        let (tb, tally) = start_fan_app().await?;
        publish_jobs(&tb, &[b"1", b"2"]).await?;
        publish_jobs(&tb, &[b"3", b"4", b"5"]).await?;
        // The first outcome waits for its delayed copy. An outcome with no `.error` goes on at
        // once; one with an `.error` waits, though every copy of it would fail now.
        publish_jobs(&tb, &[b"z"]).await?;
        assert_eq!(
            tally.0.acked.get(),
            1,
            "an outcome without an `.error` does not wait"
        );
        publish_jobs(&tb, &[b"6", b"7", b"8"]).await?;
        assert_eq!(tally.0.errors.get(), 0, "neither outcome has been told");

        // The first copy goes out and its outcome is told. Of the next outcome's copies one takes
        // the record that is free again and two find none.
        tb.advance(LATER).await?;
        tb.advance(Duration::from_secs(1)).await?;
        assert_eq!(tally.0.errors.get(), 1);
        assert_eq!(tally.0.lost.get(), 45);
        tb.advance(LATER).await?;
        assert_eq!(tally.0.errors.get(), 2);
        assert_eq!(tally.0.lost.get(), 4_578, "each call read its own failures");
        tb.shutdown().await?;
        Ok(())
    })
}
