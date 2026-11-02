//! A batch handler of the embedded runner whose mount names where its retry copies go with
//! `out_retry`: the copies the handler asks for, whole or per element, wait in the mount's store
//! and are published through the policy to that destination, not back to the subscription.
//! Driven through `TestApp` on embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several jobs is
//! made the way a busy bus makes it: the first job holds the handler for a second of the mock
//! clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, SubscriberSettings};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the handlers ask their retries for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Holds the handler for a second of the mock clock on a `gate` job, so the jobs behind it make
/// one batch.
async fn hold_on_gate(jobs: &[Job<'_>]) {
    if jobs.iter().any(|job| job.0 == b"gate") {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
}

/// Asks for one retry of the whole batch.
#[subscriber(LocalMemorySource::new("whole"))]
async fn on_whole(jobs: &[Job<'_>]) -> HandlerOutcome {
    hold_on_gate(jobs).await;
    if jobs.iter().any(|job| job.0 == b"gate") {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(LATER)
    }
}

/// Asks a retry for every job but `ok`, which it acknowledges.
#[subscriber(LocalMemorySource::new("each"))]
async fn on_each(jobs: &[Job<'_>]) -> Vec<HandlerOutcome> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| match job.0 {
            b"ok" | b"gate" => HandlerOutcome::ack(),
            _ => HandlerOutcome::retry_after(LATER),
        })
        .collect()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_whole.batch(nonzero!(3)))
            .out_retry(LocalMemoryPublish)
            .to("whole.later")
            .retry_capacity::<3>()
            .include(on_each.batch(nonzero!(3)))
            .out_retry(LocalMemoryPublish)
            .to("each.later")
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
            .message(&Sent(job))
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn the_copies_of_a_whole_batch_retry_reach_the_destination_the_mount_named()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        publish_batch(&tb, "whole", [b"a", b"b", b"c"]).await?;

        tb.broker::<Bus>()
            .published::<()>("whole.later")
            .assert_called(0);
        tb.advance(LATER).await?;

        tb.broker::<Bus>()
            .published::<()>("whole.later")
            .assert_called(3);
        tb.broker::<Bus>()
            .subscriber("whole")
            .assert_batch_sizes(&[1, 3]);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn only_the_elements_that_retry_are_copied_to_the_destination_the_mount_named()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        publish_batch(&tb, "each", [b"a", b"ok", b"c"]).await?;
        tb.advance(LATER).await?;

        let copies = tb.broker::<Bus>().published::<()>("each.later");
        copies.assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}
