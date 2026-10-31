//! What a delayed retry does when its mount's store of copies is full, per the policy the mount
//! site chose: a panic by default, the dead-letter destination, or the copy published at once.
//! Driven through `TestApp` on embassy-time's mock clock.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::sync::Mutex;
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{
    DeadLetter, EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER, SkipDelay,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retries for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
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

/// Asks for a retry later on every delivery.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_failing_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry_after(LATER)
}

/// Takes what the dead-letter destination receives.
#[subscriber(LocalMemorySource::new("jobs.dead"))]
async fn on_dead(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn default_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[ruststream::app(runtime = embassy)]
fn dead_letter_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_dead)
            .include(on_job)
            .max_attempts(nonzero!(3u32))
            .dead_letter("jobs.dead")
            .retry_capacity::<1>()
            .when_retry_full(DeadLetter)
    })
}

#[ruststream::app(runtime = embassy)]
fn skip_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_job)
            .retry_capacity::<1>()
            .when_retry_full(SkipDelay)
    })
}

#[ruststream::app(runtime = embassy)]
fn capped_skip_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_dead)
            .include(on_failing_job)
            .max_attempts(nonzero!(2u32))
            .dead_letter("jobs.dead")
            .retry_capacity::<1>()
            .when_retry_full(SkipDelay)
    })
}

/// Publishes two jobs to `jobs`: the first takes the one record, the second finds it taken.
macro_rules! publish_two {
    ($tb:expr) => {
        for job in [b"one", b"two"] {
            $tb.broker::<Bus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }
    };
}

#[test]
#[should_panic(
    expected = "the store of retry copies of subscription `jobs` is full (capacity 1) while a \
                `embedded_retry_full::Job<'_>` waits for a retry"
)]
fn a_full_store_panics_unless_the_mount_chose_a_policy() {
    let _ = block_on(async {
        let tb = TestApp::start(default_app(Bus::new())).await?;
        publish_two!(tb);
        tb.advance(Duration::from_secs(1)).await?;
        Ok::<_, Box<dyn Error>>(())
    });
}

#[test]
fn a_full_store_sends_the_delivery_to_the_dead_letter_destination() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(dead_letter_app(Bus::new())).await?;
        publish_two!(tb);

        // The second job finds the record taken and goes to the dead letter at once; its original
        // is settled, so the bus holds nothing of it.
        tb.advance(Duration::from_secs(1)).await?;
        let bus = tb.broker::<Bus>();
        bus.subscriber("jobs").assert_called(2);
        bus.subscriber("jobs.dead").assert_called_once();

        // The first job's copy is the only one that comes back.
        tb.advance(LATER).await?;
        let bus = tb.broker::<Bus>();
        bus.subscriber("jobs").assert_called(3);
        bus.subscriber("jobs.dead").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_full_store_publishes_the_copy_at_once_when_the_delay_is_skipped() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let tb = TestApp::start(skip_app(Bus::new())).await?;
        publish_two!(tb);

        // The second job's copy, with its retry count, is handled without waiting out `LATER`.
        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(3);

        tb.advance(LATER).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(4);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_skipped_delay_still_counts_toward_the_cap() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(capped_skip_app(Bus::new())).await?;
        publish_two!(tb);

        // The second job's copy is its second attempt, which is the cap: it is dead-lettered.
        tb.advance(Duration::from_secs(1)).await?;
        let bus = tb.broker::<Bus>();
        bus.subscriber("jobs").assert_called(3);
        bus.subscriber("jobs.dead").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

/// A bus whose subscription holds one message, waiting or in hand.
type TightBus = LocalMemoryBroker<1>;

/// How often the handler saw each job of the tight board, by its first byte.
#[derive(Default)]
struct Seen(Mutex<[u32; 2]>);

/// Fails the first delivery of a job and acks the next; job `a` fails its first two.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_tight_job(job: &Job<'_>, ctx: &mut Context<'_, (), Seen>) -> HandlerOutcome {
    let index = usize::from(job.0[0] - b'a');
    let mut counts = ctx.state().0.lock().expect("the handler never panics");
    counts[index] += 1;
    let failures = if index == 0 { 2 } else { 1 };
    if counts[index] <= failures {
        HandlerOutcome::retry_after(LATER)
    } else {
        HandlerOutcome::ack()
    }
}

#[ruststream::app(runtime = embassy)]
fn tight_skip_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Seen>> {
    EmbeddedApp::new()
        .with_state(Seen::default())
        .with_broker(bus, |b| {
            b.include(on_tight_job)
                .retry_capacity::<1>()
                .when_retry_full(SkipDelay)
        })
}

/// Holds its delivery for an hour of the mock clock, so the dead-letter bus stays full of it.
#[subscriber(LocalMemorySource::new("jobs.dead"))]
async fn on_stuck(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    Timer::after(EmbassyDuration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn tight_dead_letter_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Seen>> {
    EmbeddedApp::new()
        .with_state(Seen::default())
        .with_broker(bus, |b| {
            b.include(on_stuck)
                .include(on_tight_job)
                .max_attempts(nonzero!(9u32))
                .dead_letter("jobs.dead")
                .retry_capacity::<1>()
                .when_retry_full(DeadLetter)
        })
}

/// `b` fails while `a` holds the one record, and the bus the spilled copy goes to has no room: the
/// copy is refused, so `b` goes back to the broker and is handled again instead of being lost.
/// `blocked` is a destination to fill first, or none when the copy goes to the full subscription.
macro_rules! a_refused_spill_requeues_the_original {
    ($app:expr, $blocked:expr) => {
        block_on(async {
            let tb = TestApp::with_state($app(TightBus::new()), |_| Seen::default()).await?;
            if let Some(destination) = $blocked {
                tb.broker::<TightBus>()
                    .message(&Sent(b"x"))
                    .to(destination)
                    .publish()
                    .await?;
            }
            for job in [b"a", b"b"] {
                tb.broker::<TightBus>()
                    .message(&Sent(job))
                    .to("jobs")
                    .publish()
                    .await?;
            }

            tb.advance(Duration::from_secs(1)).await?;
            tb.broker::<TightBus>().subscriber("jobs").assert_called(3);

            // `a`'s copy falls due and fails again; the next one is handled to its end.
            tb.advance(LATER).await?;
            tb.broker::<TightBus>().subscriber("jobs").assert_called(4);
            tb.advance(LATER).await?;
            tb.broker::<TightBus>().subscriber("jobs").assert_called(5);
            tb.shutdown().await?;
            Ok(())
        })
    };
}

#[test]
fn a_skipped_copy_the_bus_refuses_requeues_the_original() -> Result<(), Box<dyn Error>> {
    a_refused_spill_requeues_the_original!(tight_skip_app, None::<&str>)
}

#[test]
fn a_dead_letter_the_bus_refuses_requeues_the_original() -> Result<(), Box<dyn Error>> {
    a_refused_spill_requeues_the_original!(tight_dead_letter_app, Some("jobs.dead"))
}
