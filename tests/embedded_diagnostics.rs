//! What a device service's runtime says about a lost message or a continuation that never runs,
//! read from the `log` records the facade's `log` feature turns on, in a build without `alloc`.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::error::Error;
use std::sync::{Mutex, Once, PoisonError};
use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use log::{Level, LevelFilter, Log, Metadata, Record};
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{EmbeddedApp, Failures, HandlerOutcome, Mounts, SubscriberSettings};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// Keeps the warnings and errors the framework logs, as text.
struct Records(Mutex<Vec<String>>);

impl Log for Records {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Warn
    }

    fn log(&self, record: &Record<'_>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!(
                "{} {}: {}",
                record.level(),
                record.target(),
                record.args()
            ));
    }

    fn flush(&self) {}
}

/// The process's one logger: the `log` crate takes a single one.
static RECORDS: Records = Records(Mutex::new(Vec::new()));

/// Routes the framework's records into [`RECORDS`], once.
fn record() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        log::set_logger(&RECORDS).expect("no other logger in this binary");
        log::set_max_level(LevelFilter::Warn);
    });
}

/// The records that mention `subscription`.
fn about(subscription: &str) -> Vec<String> {
    let needle = format!("subscription={subscription}");
    RECORDS
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|line| line.contains(&needle))
        .cloned()
        .collect()
}

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A question for the board, and the answer the board cannot place.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "quoted")]
struct Quote(&'static [u8]);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(&'static [u8]);

/// Holds its delivery for an hour of the clock, so the answers subscription of one message stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_stuck(held: &Job<'_>) -> HandlerOutcome {
    let _ = held.0;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// Answers a quote to a bus that is full, with no `.error` to take the refused answer.
#[subscriber(LocalMemorySource::new("quoted"), reply)]
async fn on_quote(quote: &Job<'_>) -> Answer {
    let _ = quote.0;
    Answer(b"yes")
}

/// A subscription of one message, so the answers can fill it.
type Tight = LocalMemoryBroker<1>;

/// The bus of this test.
static TIGHT_BUS: LocalMemoryBus<1> = LocalMemoryBus::new();

fn quoted_app(bus: Tight) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_stuck)
            .include(on_quote)
            .out_reply(LocalMemoryPublish)
    })
}

#[test]
fn a_reply_the_bus_refuses_is_logged_with_its_subscription_and_message_type()
-> Result<(), Box<dyn Error>> {
    record();
    block_on(async {
        let tb = TestApp::start(quoted_app(Tight::new(&TIGHT_BUS))).await?;
        // The first answer fills the one slot of the answers subscription for good.
        tb.broker::<Tight>()
            .message(&Answer(b"x"))
            .publish()
            .await?;
        tb.broker::<Tight>().message(&Quote(b"?")).publish().await?;
        tb.advance(Duration::from_secs(1)).await?;
        tb.shutdown().await?;
        Ok::<(), Box<dyn Error>>(())
    })?;

    let lines = about("quoted");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("WARN ruststream::dispatch:"),
        "{lines:?}"
    );
    assert!(lines[0].contains("message lost"), "{lines:?}");
    assert!(lines[0].contains("destination=answers"), "{lines:?}");
    assert!(
        lines[0].contains("message_type=embedded_diagnostics::Answer"),
        "{lines:?}"
    );
    Ok(())
}

/// Takes the jobs that arrived together and asks for a reaction to a failed copy, which a batch
/// route cannot give.
#[subscriber(LocalMemorySource::new("batched"))]
async fn on_batched(jobs: &[Job<'_>]) -> HandlerOutcome {
    let _ = jobs.len();
    HandlerOutcome::drop().error(async |_failures: &Failures| {})
}

/// A job for the batch.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "batched")]
struct Batched(&'static [u8]);

/// The bus of the batch scenario.
static BATCH_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// The stack the batch scenario runs on: a batch's storage and its futures are large in a build
/// without optimisation, more than the test runner's default.
const BATCH_STACK: usize = 8 * 1024 * 1024;

#[test]
fn an_error_on_a_batch_outcome_is_logged_as_never_running() -> Result<(), Box<dyn Error>> {
    record();
    let scenario = thread::Builder::new().stack_size(BATCH_STACK).spawn(|| {
        block_on(async {
            let scenario = async {
                let app = EmbeddedApp::new()
                    .with_broker(LocalMemoryBroker::<4>::new(&BATCH_BUS), |b| {
                        b.include(on_batched.batch::<3>())
                    });
                let tb = TestApp::start(app).await?;
                for _ in 0..3 {
                    tb.broker::<LocalMemoryBroker<4>>()
                        .message(&Batched(b"job"))
                        .publish()
                        .await?;
                }
                tb.shutdown().await?;
                Ok::<(), Box<dyn Error>>(())
            };
            scenario.await.map_err(|err| err.to_string())
        })
    })?;
    scenario
        .join()
        .map_err(|_| "the batch scenario panicked")??;

    // One warning per delivered batch, however the bus groups the three jobs.
    let lines = about("batched");
    assert!(!lines.is_empty(), "{lines:?}");
    assert!(
        lines
            .iter()
            .all(|line| line.contains("the continuation never runs")),
        "{lines:?}"
    );
    Ok(())
}
