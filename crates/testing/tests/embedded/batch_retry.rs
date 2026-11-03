//! A delayed retry of a batch handler on the embedded runner: the copies the handler asks for,
//! whole or per element, wait in the mount's store and come back after their delay. Driven through
//! `TestApp` on embassy-time's mock clock.
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
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, SubscriberSettings,
};
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

/// What the `.error` continuations were told: where the lost copy was going, and why.
#[derive(Clone, Default)]
struct Lost(Arc<Mutex<Vec<(String, String)>>>);

impl Lost {
    fn told(&self) -> Vec<(String, String)> {
        self.0.lock().expect("the handlers never panic").clone()
    }
}

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

/// Asks a retry for every job but `ok`, and records the copy it loses.
#[subscriber(LocalMemorySource::new("lossy"))]
async fn on_lossy(jobs: &[Job<'_>], ctx: &mut Context<'_, (), Lost>) -> Vec<HandlerOutcome> {
    hold_on_gate(jobs).await;
    jobs.iter()
        .map(|job| match job.0 {
            b"ok" | b"gate" => HandlerOutcome::ack(),
            _ => {
                let lost = ctx.state().clone();
                HandlerOutcome::retry_after(LATER).error(async move |failures: Failures| {
                    for failed in failures {
                        let (error, message) = failed.into_parts();
                        lost.0
                            .lock()
                            .expect("the handlers never panic")
                            .push((message.destination().to_owned(), error.to_string()));
                    }
                })
            }
        })
        .collect()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_whole.batch(nonzero!(3)))
            .retry_capacity::<3>()
            .include(on_each.batch(nonzero!(3)))
            .retry_capacity::<3>()
    })
}

#[ruststream::app(runtime = embassy)]
fn lossy_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.include(on_lossy.batch(nonzero!(3))).retry_capacity::<3>()
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
fn a_whole_batch_retry_copies_every_element_and_redelivers_them_after_the_delay()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        publish_batch(&tb, "whole", [b"a", b"b", b"c"]).await?;

        tb.broker::<Bus>()
            .subscriber("whole")
            .assert_batch_sizes(&[1, 3]);
        tb.advance(Duration::from_secs(4)).await?;
        tb.broker::<Bus>()
            .subscriber("whole")
            .assert_batch_sizes(&[1, 3]);
        tb.advance(Duration::from_secs(1)).await?;
        let batches = tb.broker::<Bus>().subscriber("whole").batches_raw();
        assert_eq!(
            batches
                .last()
                .map(|batch| batch.iter().map(|one| one.to_vec()).collect::<Vec<_>>()),
            Some(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
            "the copies come back together, in the order of their batch",
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn elements_with_mixed_outcomes_copy_only_the_ones_that_retry_each_once_in_order()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        publish_batch(&tb, "each", [b"a", b"ok", b"c"]).await?;

        tb.advance(LATER).await?;
        let batches = tb.broker::<Bus>().subscriber("each").batches_raw();
        let copies: Vec<Vec<u8>> = batches
            .iter()
            .skip(2)
            .flatten()
            .map(|one| one.to_vec())
            .collect();
        assert_eq!(
            copies,
            [b"a".to_vec(), b"c".to_vec()],
            "each retried element is back once, in element order, and the acked one is not",
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_element_that_finds_the_store_full_is_told_through_its_error_continuation()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(lossy_app(Bus::new()), move |_| shared).await?;
        publish_batch(&tb, "lossy", [b"a", b"b", b"c"]).await?;
        // The three records hold the first batch's copies; the next batch finds none free, and
        // its other element goes on.
        publish_batch(&tb, "lossy", [b"d", b"ok", b"e"]).await?;

        assert_eq!(
            lost.told(),
            [
                ("lossy".to_owned(), NO_FREE_RECORD.to_owned()),
                ("lossy".to_owned(), NO_FREE_RECORD.to_owned()),
            ],
            "the two retried elements of the second batch lost their copies",
        );
        tb.shutdown().await?;
        Ok(())
    })
}

const NO_FREE_RECORD: &str = "no free record in the store of retry copies (capacity 3)";
