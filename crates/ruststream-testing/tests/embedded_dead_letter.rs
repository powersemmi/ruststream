//! `max_attempts` over delayed retries of a device service's handler: the copies the runtime
//! publishes spend the cap, and the delivery that reaches it goes to the dead-letter destination.
//! Driven through `TestApp` on embassy-time's mock clock.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::time::Duration;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
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

/// Asks for a retry later on every delivery.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry_after(LATER)
}

/// Takes what the cap sent away.
#[subscriber(LocalMemorySource::new("jobs.dead"))]
async fn on_dead(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_dead)
            .include(on_job)
            .max_attempts(nonzero!(2u32))
            .dead_letter("jobs.dead")
            .retry_capacity::<1>()
    })
}

#[test]
fn the_cap_counts_the_runtimes_copies_and_sends_the_last_try_to_the_dead_letter()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"job"))
            .to("jobs")
            .publish()
            .await?;

        tb.advance(LATER).await?;
        tb.advance(LATER).await?;
        tb.advance(LATER).await?;
        let bus = tb.broker::<Bus>();
        bus.subscriber("jobs").assert_called(2);
        bus.subscriber("jobs.dead").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
