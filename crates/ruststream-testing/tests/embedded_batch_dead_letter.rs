//! `max_attempts` over the delayed retries of a batch handler: the copies the runtime publishes
//! spend the cap per element, and the element that reaches it goes to the dead-letter destination
//! while its batch-mates retry. Driven through `TestApp` on embassy-time's mock clock.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, SubscriberSettings};
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

/// Asks a retry for every job, one outcome each; a `gate` job holds the handler for a second of
/// the mock clock so the jobs behind it make one batch.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_jobs(jobs: &[Job<'_>]) -> Vec<HandlerOutcome> {
    if jobs.iter().any(|job| job.0 == b"gate") {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
    jobs.iter()
        .map(|job| match job.0 {
            b"gate" => HandlerOutcome::ack(),
            _ => HandlerOutcome::retry_after(LATER),
        })
        .collect()
}

/// Asks one retry for the whole batch.
#[subscriber(LocalMemorySource::new("whole"))]
async fn on_whole(jobs: &[Job<'_>]) -> HandlerOutcome {
    let _ = jobs.len();
    HandlerOutcome::retry_after(LATER)
}

/// Takes what the cap sent away.
#[subscriber(LocalMemorySource::new("dead"))]
async fn on_dead(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_dead)
            .include(on_jobs.batch(nonzero!(3)))
            .max_attempts(nonzero!(2u32))
            .dead_letter("dead")
            .retry_capacity::<3>()
            .include(on_whole.batch(nonzero!(3)))
            .max_attempts(nonzero!(2u32))
            .dead_letter("dead")
            .retry_capacity::<3>()
    })
}

#[test]
fn the_element_that_reaches_the_cap_goes_to_the_dead_letter_while_its_batch_mate_retries()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"a"))
            .to("jobs")
            .publish()
            .await?;
        // `a` comes back at five seconds, behind `n`, and the two are handled as one batch.
        tb.advance(Duration::from_millis(4500)).await?;
        for job in [&b"gate"[..], &b"n"[..]] {
            tb.broker::<Bus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }
        // The copy is published at five seconds, while the gate still holds the handler.
        tb.advance(Duration::from_millis(600)).await?;
        tb.advance(Duration::from_millis(500)).await?;

        let bus = tb.broker::<Bus>();
        bus.subscriber("jobs").assert_batch_sizes(&[1, 1, 2]);
        let dead = bus.subscriber("dead").assert_called_once().received_raw();
        assert_eq!(
            dead.iter().map(|one| one.to_vec()).collect::<Vec<_>>(),
            [b"a"]
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_whole_batch_that_reaches_the_cap_sends_every_element_to_the_dead_letter()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"a"))
            .to("whole")
            .publish()
            .await?;
        tb.advance(LATER).await?;
        tb.advance(LATER).await?;

        let bus = tb.broker::<Bus>();
        bus.subscriber("whole").assert_called(2);
        bus.subscriber("dead").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
