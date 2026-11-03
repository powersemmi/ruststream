//! The `.error` of a batch handler's uniform outcome on the embedded runner: one call with the
//! failure of every copy that did not go out, in the order of the batch, once the last copy is
//! reported. Driven through `TestApp` on embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several jobs is
//! made the way a busy bus makes it: the first job holds the handler for a second of the mock
//! clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER,
    SubscriberSettings,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// How long the first call's copies hold their records: past every scenario below.
const FILL: Duration = Duration::from_secs(60);

/// The delay of the uniform outcomes under test.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// What the uniform outcomes were told: handler calls, `.error` calls, the payload digits of the
/// failures in the order they were read, and the follow-ups that ran.
#[derive(Clone, Default)]
struct Tally(Arc<TallyCells>);

#[derive(Default)]
struct TallyCells {
    calls: AtomicU32,
    errors: AtomicU32,
    lost: AtomicU32,
    thens: AtomicU32,
}

/// Holds the handler for a second of the mock clock on a `gate` job.
async fn hold_on_gate(jobs: &[Job<'_>]) {
    if jobs.iter().any(|job| job.0 == b"gate") {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
}

/// The first call asks a retry for the batch and reads no failure; later calls ask the same with
/// an `.error` that reads every failed copy and a `.then`.
#[subscriber(LocalMemorySource::new("fan"))]
async fn on_fan(jobs: &[Job<'_>], ctx: &mut Context<'_, (), Tally>) -> HandlerOutcome {
    hold_on_gate(jobs).await;
    if jobs.iter().any(|job| job.0 == b"gate")
        || ctx.headers().get_str(RETRY_COUNT_HEADER).is_some()
    {
        return HandlerOutcome::ack();
    }
    let tally = ctx.state().clone();
    if tally.0.calls.fetch_add(1, Ordering::SeqCst) == 0 {
        return HandlerOutcome::retry_after(FILL);
    }
    let after = tally.clone();
    HandlerOutcome::retry_after(LATER)
        .and_after(async move {
            after.0.thens.fetch_add(1, Ordering::SeqCst);
        })
        .error(async move |failures: Failures| {
            tally.0.errors.fetch_add(1, Ordering::SeqCst);
            for failed in failures {
                let digit = failed
                    .message()
                    .payload()
                    .and_then(|payload| payload.first().copied())
                    .map_or(0, |byte| u32::from(byte - b'0'));
                let seen = tally.0.lost.load(Ordering::SeqCst);
                tally.0.lost.store(seen * 10 + digit, Ordering::SeqCst);
            }
        })
}

#[ruststream::app(runtime = embassy)]
fn fan_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Tally>> {
    EmbeddedApp::new()
        .with_state(Tally::default())
        .with_broker(bus, |b| {
            b.include(on_fan.batch(nonzero!(3))).retry_capacity::<3>()
        })
}

/// Starts the app with a tally the test keeps a handle to.
async fn start_fan_app() -> Result<(TestApp, Tally), Box<dyn Error>> {
    let tally = Tally::default();
    let shared = tally.clone();
    let tb = TestApp::with_state(fan_app(Bus::new()), move |_| shared).await?;
    Ok((tb, tally))
}

/// Publishes a gate, then `jobs` behind it, and lets the gate open: the jobs arrive as one batch.
async fn publish_jobs(tb: &TestApp, jobs: &[&'static [u8]]) -> Result<(), Box<dyn Error>> {
    for job in once(&b"gate"[..]).chain(jobs.iter().copied()) {
        tb.broker::<Bus>()
            .message(&Sent(job))
            .to("fan")
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn a_uniform_error_reads_every_failed_copy_of_the_batch_once_after_the_last_copy_is_reported()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let (tb, tally) = start_fan_app().await?;
        // Two records go to the first batch's copies, so of the next three jobs one waits for its
        // delay and two find no record.
        publish_jobs(&tb, &[b"1", b"2"]).await?;
        publish_jobs(&tb, &[b"3", b"4", b"5"]).await?;
        assert_eq!(
            tally.0.errors.load(Ordering::SeqCst),
            0,
            "the delayed copy has not been reported"
        );

        tb.advance(LATER).await?;
        assert_eq!(
            tally.0.errors.load(Ordering::SeqCst),
            1,
            "one call for the batch"
        );
        assert_eq!(
            tally.0.lost.load(Ordering::SeqCst),
            45,
            "both failures, in the order of the batch"
        );
        assert_eq!(
            tally.0.thens.load(Ordering::SeqCst),
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
    block_on(async {
        let (tb, tally) = start_fan_app().await?;
        publish_jobs(&tb, &[b"1", b"2", b"3"]).await?;
        publish_jobs(&tb, &[b"4", b"5", b"6"]).await?;

        assert_eq!(tally.0.errors.load(Ordering::SeqCst), 1);
        assert_eq!(tally.0.lost.load(Ordering::SeqCst), 456);
        assert_eq!(tally.0.thens.load(Ordering::SeqCst), 0, "no copy went out");
        tb.shutdown().await?;
        Ok(())
    })
}
