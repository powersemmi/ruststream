//! Continuations that run after a delivery is settled, on a device service's app: the outcome's
//! `and_after` and the context's `after(..).then(..)`. Driven through `TestApp` on embassy-time's
//! mock clock; each continuation adds its job's weight to a total the app's state holds.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::Timer;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// What the continuations added up.
type Total = Arc<AtomicU32>;

/// A job, as its bytes; the first byte is its weight.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Acks the job and leaves a continuation that adds its weight.
#[subscriber(LocalMemorySource::new("jobs/outcome"))]
async fn on_outcome(job: &Job<'_>, ctx: &mut Context<'_, (), Total>) -> HandlerOutcome {
    let (weight, total) = (u32::from(job.0[0]), Arc::clone(ctx.state()));
    HandlerOutcome::ack().and_after(async move {
        total.fetch_add(weight, Ordering::Relaxed);
    })
}

/// Acks the job and registers a context hook that adds its weight.
#[subscriber(LocalMemorySource::new("jobs/hook"))]
async fn on_hook(job: &Job<'_>, ctx: &mut Context<'_, (), Total>) -> HandlerOutcome {
    let (weight, total) = (u32::from(job.0[0]), Arc::clone(ctx.state()));
    ctx.after(HandlerOutcome::ack()).then(async move {
        total.fetch_add(weight, Ordering::Relaxed);
    });
    HandlerOutcome::ack()
}

/// Drops the job and registers a hook gated on an ack.
#[subscriber(LocalMemorySource::new("jobs/gated"))]
async fn on_gated(job: &Job<'_>, ctx: &mut Context<'_, (), Total>) -> HandlerOutcome {
    let (weight, total) = (u32::from(job.0[0]), Arc::clone(ctx.state()));
    ctx.after(HandlerOutcome::ack()).then(async move {
        total.fetch_add(weight, Ordering::Relaxed);
    });
    HandlerOutcome::drop()
}

/// Leaves a continuation that waits three seconds on the board's clock before it adds.
#[subscriber(LocalMemorySource::new("jobs/late"))]
async fn on_late(job: &Job<'_>, ctx: &mut Context<'_, (), Total>) -> HandlerOutcome {
    let (weight, total) = (u32::from(job.0[0]), Arc::clone(ctx.state()));
    HandlerOutcome::ack().and_after(async move {
        Timer::after_secs(3).await;
        total.fetch_add(weight, Ordering::Relaxed);
    })
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Total>> {
    EmbeddedApp::new()
        .with_state(Total::default())
        .with_broker(bus, |b| {
            b.include(on_outcome)
                .include(on_hook)
                .include(on_gated)
                .include(on_late)
        })
}

/// Starts the app with a total the test keeps a handle to.
async fn start() -> Result<(TestApp, Total), Box<dyn Error>> {
    let total = Total::default();
    let held = Arc::clone(&total);
    let tb = TestApp::with_state(app(Bus::new()), move |_| held).await?;
    Ok((tb, total))
}

#[test]
fn an_outcome_continuation_runs_once_the_job_is_settled() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let (tb, total) = start().await?;
        tb.broker::<Bus>()
            .message(&Sent(&[7]))
            .to("jobs/outcome")
            .publish()
            .await?;

        assert_eq!(total.load(Ordering::Relaxed), 7);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_context_hook_runs_once_the_job_is_settled() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let (tb, total) = start().await?;
        tb.broker::<Bus>()
            .message(&Sent(&[5]))
            .to("jobs/hook")
            .publish()
            .await?;

        assert_eq!(total.load(Ordering::Relaxed), 5);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_hook_gated_on_an_ack_stays_silent_when_the_job_is_dropped() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let (tb, total) = start().await?;
        tb.broker::<Bus>()
            .message(&Sent(&[9]))
            .to("jobs/gated")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("jobs/gated")
            .assert_called_once();
        assert_eq!(total.load(Ordering::Relaxed), 0);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_continuation_waiting_on_the_clock_runs_when_the_clock_reaches_it() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let (tb, total) = start().await?;
        tb.broker::<Bus>()
            .message(&Sent(&[3]))
            .to("jobs/late")
            .publish()
            .await?;
        assert_eq!(total.load(Ordering::Relaxed), 0);

        tb.advance(Duration::from_secs(3)).await?;
        assert_eq!(total.load(Ordering::Relaxed), 3);
        tb.shutdown().await?;
        Ok(())
    })
}
