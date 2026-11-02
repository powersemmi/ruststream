//! Continuations of a batch handler on the embedded runner: the `and_after` of a whole batch runs
//! once after the batch is settled, and the `and_after` of each element runs once its element is,
//! in the order of the batch. Driven through `TestApp` on embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several jobs is
//! made the way a busy bus makes it: the first job holds the handler for a second of the mock
//! clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, SubscriberSettings};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// What the continuations saw, in the order they ran.
type Ran = Arc<Mutex<Vec<String>>>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Holds the handler for a second of the mock clock on a `gate` job.
async fn hold_on_gate(jobs: &[Job<'_>]) {
    if jobs.iter().any(|job| job.0 == b"gate") {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
}

/// Acks the batch and leaves one continuation for all of it.
#[subscriber(LocalMemorySource::new("whole"))]
async fn on_whole(jobs: &[Job<'_>], ctx: &mut Context<'_, (), Ran>) -> HandlerOutcome {
    hold_on_gate(jobs).await;
    if jobs.iter().any(|job| job.0 == b"gate") {
        return HandlerOutcome::ack();
    }
    let (ran, size) = (Arc::clone(ctx.state()), jobs.len());
    HandlerOutcome::ack().and_after(async move {
        ran.lock()
            .expect("the handlers never panic")
            .push(format!("batch of {size}"));
    })
}

/// Acks every job and leaves a continuation for each that waits a second on the board's clock.
#[subscriber(LocalMemorySource::new("each"))]
async fn on_each(jobs: &[Job<'_>], ctx: &mut Context<'_, (), Ran>) -> Vec<HandlerOutcome> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| {
            if job.0 == b"gate" {
                return HandlerOutcome::ack();
            }
            let (ran, name) = (
                Arc::clone(ctx.state()),
                String::from_utf8_lossy(job.0).into_owned(),
            );
            HandlerOutcome::ack().and_after(async move {
                Timer::after(EmbassyDuration::from_secs(1)).await;
                ran.lock().expect("the handlers never panic").push(name);
            })
        })
        .collect()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Ran>> {
    EmbeddedApp::new()
        .with_state(Ran::default())
        .with_broker(bus, |b| {
            b.include(on_whole.batch(nonzero!(3)))
                .include(on_each.batch(nonzero!(3)))
        })
}

/// Starts the app with a record the test keeps a handle to.
async fn start() -> Result<(TestApp, Ran), Box<dyn Error>> {
    let ran = Ran::default();
    let held = Arc::clone(&ran);
    let tb = TestApp::with_state(app(Bus::new()), move |_| held).await?;
    Ok((tb, ran))
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
            .message(&Sent(job))
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn a_uniform_continuation_runs_once_after_the_whole_batch_is_settled() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let (tb, ran) = start().await?;
        publish_batch(&tb, "whole", [b"a", b"b", b"c"]).await?;

        tb.broker::<Bus>()
            .subscriber("whole")
            .assert_batch_sizes(&[1, 3]);
        assert_eq!(*ran.lock().expect("no panic"), ["batch of 3"]);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_element_continuation_runs_in_the_order_of_the_batch_and_none_is_dropped()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let (tb, ran) = start().await?;
        publish_batch(&tb, "each", [b"a", b"b", b"c"]).await?;

        // The worker holds one continuation at a time, so each element waits its turn.
        tb.advance(Duration::from_secs(3)).await?;
        assert_eq!(*ran.lock().expect("no panic"), ["a", "b", "c"]);
        tb.shutdown().await?;
        Ok(())
    })
}
