//! The embedded runner of the heap variant reports through the logger the app selects and through
//! nothing else: with no logger a refused reply and an unsupported acknowledgement are silent,
//! `TracingLogger` and `LogLogger` each deliver them with their subscription and message type, and
//! the level the user's backend filters at silences or shows them.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(ruststream_alloc)]

use std::error::Error;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::executor::block_on;
use log::LevelFilter;
use ruststream::diagnostics::{LogLogger, TracingLogger};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use tracing::Level;
use tracing::subscriber::set_default;

mod logger_capture;
use logger_capture::{RECORDS, Traced, rank, take_records};

#[path = "embedded_unacknowledged/mod.rs"]
mod unacknowledged;

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

/// Holds its delivery for an hour of the clock, so the answers subscription of one message stays
/// full.
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

/// No lines.
const NONE: [String; 0] = [];

/// The logger an app selects.
#[derive(Clone, Copy)]
enum Choice {
    Nothing,
    Tracing,
    Log,
}

fn quoted_app(bus: Tight) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_stuck)
            .include(on_quote)
            .out_reply(LocalMemoryPublish)
    })
}

/// Runs the app whose answer the bus refuses, with `choice` selecting the logger.
async fn refuse_a_reply(choice: Choice) -> Result<(), Box<dyn Error>> {
    let app = quoted_app(Tight::new());
    let app = match choice {
        Choice::Nothing => app,
        Choice::Tracing => app.logger(TracingLogger),
        Choice::Log => app.logger(LogLogger),
    };
    let tb = TestApp::start(app).await?;
    // The first answer fills the one slot of the answers subscription for good.
    tb.broker::<Tight>()
        .message(&Answer(b"x"))
        .publish()
        .await?;
    tb.broker::<Tight>().message(&Quote(b"?")).publish().await?;
    tb.advance(Duration::from_secs(1)).await?;
    tb.shutdown().await?;
    Ok(())
}

/// Runs the app whose transport answers every settlement unsupported.
async fn lose_an_acknowledgement(choice: Choice) -> Result<(), Box<dyn Error>> {
    match choice {
        Choice::Nothing => unacknowledged::run_unselected().await,
        Choice::Tracing => unacknowledged::run(TracingLogger).await,
        Choice::Log => unacknowledged::run(LogLogger).await,
    }
}

/// The warnings among `lines`: the lifecycle events arrive too.
fn warnings(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter(|line| line.starts_with("WARN"))
        .cloned()
        .collect()
}

fn assert_refused_reply(lines: &[String]) {
    let lines = warnings(lines);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("WARN ruststream::dispatch:"),
        "{lines:?}"
    );
    assert!(lines[0].contains("message lost"), "{lines:?}");
    assert!(lines[0].contains("subscription=quoted"), "{lines:?}");
    assert!(lines[0].contains("destination=answers"), "{lines:?}");
    assert!(
        lines[0].contains("message_type=embedded_diagnostics_alloc::Answer"),
        "{lines:?}"
    );
}

fn assert_unsupported_acknowledgement(lines: &[String]) {
    let lines = warnings(lines);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("WARN ruststream::dispatch:"),
        "{lines:?}"
    );
    assert!(lines[0].contains("ack / nack failed"), "{lines:?}");
    assert!(lines[0].contains("subscription=readings"), "{lines:?}");
    assert!(
        lines[0].contains("error=acknowledgement is not supported by this broker"),
        "{lines:?}"
    );
}

#[test]
fn the_framework_reports_only_through_the_logger_the_app_selected() -> Result<(), Box<dyn Error>> {
    log::set_logger(&RECORDS)?;
    log::set_max_level(LevelFilter::Debug);

    // No logger selected: neither backend hears anything.
    let traced = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(traced.clone());
        block_on(refuse_a_reply(Choice::Nothing))?;
        block_on(lose_an_acknowledgement(Choice::Nothing))?;
    }
    assert!(traced.lines().is_empty(), "{:?}", traced.lines());
    assert_eq!(take_records(), NONE);

    // `TracingLogger`: the tracing subscriber hears both, the log crate does not.
    {
        let _scope = set_default(traced.clone());
        block_on(refuse_a_reply(Choice::Tracing))?;
    }
    assert_refused_reply(&traced.lines());
    let acknowledged = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(acknowledged.clone());
        block_on(lose_an_acknowledgement(Choice::Tracing))?;
    }
    assert_unsupported_acknowledgement(&acknowledged.lines());
    assert_eq!(take_records(), NONE);

    // The subscriber's level silences the warning, and the same event shows at the next level.
    let silenced = Traced::at(Level::ERROR);
    {
        let _scope = set_default(silenced.clone());
        block_on(refuse_a_reply(Choice::Tracing))?;
    }
    assert!(silenced.lines().is_empty(), "{:?}", silenced.lines());
    silenced.level.store(rank(Level::WARN), Ordering::Relaxed);
    {
        let _scope = set_default(silenced.clone());
        block_on(refuse_a_reply(Choice::Tracing))?;
    }
    assert_refused_reply(&silenced.lines());

    // `LogLogger`: the log crate hears both, the tracing subscriber does not.
    let quiet = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(quiet.clone());
        block_on(refuse_a_reply(Choice::Log))?;
    }
    assert!(quiet.lines().is_empty(), "{:?}", quiet.lines());
    assert_refused_reply(&take_records());
    block_on(lose_an_acknowledgement(Choice::Log))?;
    assert_unsupported_acknowledgement(&take_records());

    // `log::max_level()` silences the warning, and the same event shows at the next level.
    log::set_max_level(LevelFilter::Error);
    block_on(refuse_a_reply(Choice::Log))?;
    assert_eq!(take_records(), NONE);
    log::set_max_level(LevelFilter::Warn);
    block_on(refuse_a_reply(Choice::Log))?;
    assert_refused_reply(&take_records());
    Ok(())
}
