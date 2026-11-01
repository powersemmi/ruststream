//! A device service whose store of delayed retries is full: a copy that has no record to wait in
//! is sent from the delivery itself, and one the bus refuses reaches the `.error` continuation.
//! Driven through `TestApp` on embassy-time's mock clock.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER,
};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the first job asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes; the first byte names it.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Job `a` waits for a retry later and holds the store's one record. Any other job asks for an
/// immediate retry on its first delivery and acknowledges the copy.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let counted = ctx.headers().get_str(RETRY_COUNT_HEADER).is_some();
    match (job.0[0], counted) {
        (b'a', false) => HandlerOutcome::retry_after(LATER),
        (b'a', true) => HandlerOutcome::ack(),
        (_, false) => HandlerOutcome::retry_after(Duration::ZERO),
        (_, true) => HandlerOutcome::ack(),
    }
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[test]
fn an_immediate_retry_is_sent_while_the_store_holds_a_delayed_one() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        for job in [Sent(b"a"), Sent(b"b")] {
            tb.broker::<Bus>()
                .message(&job)
                .to("jobs")
                .publish()
                .await?;
        }

        let jobs = tb.broker::<Bus>();
        let calls = jobs.subscriber("jobs").assert_called(3);
        assert_eq!(
            calls.outcomes().last(),
            Some(&Outcome::Ack),
            "the copy that skipped the full store reached the handler"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

/// A bus whose subscription holds one message, waiting or in hand.
type TightBus = LocalMemoryBroker<1>;

/// What the `.error` continuations were told: where each lost copy was going, and its bytes.
#[derive(Clone, Default)]
struct Lost(Arc<Mutex<Vec<(String, Vec<u8>)>>>);

/// Job `a` waits for a retry later and holds the store's one record; any other job asks for an
/// immediate retry on every delivery. Each learns of a copy that is lost.
#[subscriber(LocalMemorySource::new("capped"))]
async fn on_capped(job: &Job<'_>, ctx: &mut Context<'_, (), Lost>) -> HandlerOutcome {
    let lost = ctx.state().clone();
    let delay = if job.0[0] == b'a' { LATER } else { Duration::ZERO };
    HandlerOutcome::retry_after(delay).error(async move |failures: Failures| {
        for failed in failures {
            let (_, message) = failed.into_parts();
            lost.0
                .lock()
                .expect("the handlers never panic")
                .push((message.destination().to_owned(), message.payload().to_vec()));
        }
    })
}

/// Holds its delivery for an hour of the mock clock, so the bus stays full of it.
#[subscriber(LocalMemorySource::new("capped.dead"))]
async fn on_stuck(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    Timer::after(EmbassyDuration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn capped_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.include(on_stuck)
                .include(on_capped)
                .max_attempts(nonzero!(2u32))
                .dead_letter("capped.dead")
                .retry_capacity::<1>()
        })
}

#[test]
fn a_dead_letter_sent_from_a_full_store_that_the_bus_refuses_reaches_the_error_continuation()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(capped_app(TightBus::new()), move |_| shared).await?;
        // The dead-letter subscription holds this message for the hour, so it has no room.
        for (destination, job) in [("capped.dead", b"x"), ("capped", b"a"), ("capped", b"b")] {
            tb.broker::<TightBus>()
                .message(&Sent(job))
                .to(destination)
                .publish()
                .await?;
        }

        // `a` holds the one record. `b` is retried at once, and its copy reaches the cap, so
        // the dead letter leaves without a record and the bus refuses it.
        tb.advance(Duration::from_secs(1)).await?;

        assert_eq!(
            *lost.0.lock().expect("the handlers never panic"),
            [("capped.dead".to_owned(), b"b".to_vec())]
        );
        tb.shutdown().await?;
        Ok(())
    })
}

/// Asks for an immediate retry on a first delivery and acknowledges the copy.
#[subscriber(LocalMemorySource::new("plain"))]
async fn on_plain(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(Duration::ZERO)
    }
}

#[ruststream::app(runtime = embassy)]
fn plain_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_plain))
}

#[ruststream::app(runtime = embassy)]
fn tight_plain_app(bus: TightBus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_plain))
}

#[test]
fn an_immediate_retry_needs_no_store() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(plain_app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"p"))
            .to("plain")
            .publish()
            .await?;

        let plain = tb.broker::<Bus>();
        let calls = plain.subscriber("plain").assert_called(2);
        assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_immediate_retry_the_bus_refuses_is_lost() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(tight_plain_app(TightBus::new())).await?;
        // The delivery in hand fills the bus, so there is no room for its copy.
        tb.broker::<TightBus>()
            .message(&Sent(b"p"))
            .to("plain")
            .publish()
            .await?;

        tb.broker::<TightBus>()
            .subscriber("plain")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
