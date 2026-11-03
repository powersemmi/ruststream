//! The compio runner reports through the logger the app selects and through nothing else: with no
//! logger a lost reply is silent, `TracingLogger` delivers it with its subscription and message
//! type, and the level of the user's subscriber silences or shows it.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.

#![allow(clippy::future_not_send)]

use std::error::Error;
use std::fmt::{Debug, Write as _};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::time::sleep;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::subscriber::set_default;
use tracing::{Event, Level, Metadata, Subscriber};

/// A bus whose subscription holds one message, waiting or in hand.
type TightBus = LocalMemoryBroker<1>;

/// An ask, as its bytes.
#[derive(Deserialized)]
struct Ask<'a>(&'a [u8]);

/// A message the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The answer, which the default codec writes.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Verdict {
    ok: bool,
}

/// Answers an ask; the answers subscription is full, so the answer is lost.
#[subscriber(LocalMemorySource::new("ask"), reply)]
async fn on_ask(ask: &Ask<'_>) -> Verdict {
    let _ = ask.0;
    Verdict { ok: true }
}

/// Holds its delivery for an hour of the virtual clock, so the answers subscription stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers_stuck(ask: &Ask<'_>) -> HandlerOutcome {
    let _ = ask.0;
    sleep(Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// The `tracing` events a scope saw, filtered at a level the test moves.
#[derive(Clone)]
struct Traced {
    level: Arc<AtomicU8>,
    lines: Arc<Mutex<Vec<String>>>,
}

impl Traced {
    fn at(level: Level) -> Self {
        Self {
            level: Arc::new(AtomicU8::new(rank(level))),
            lines: Arc::default(),
        }
    }

    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Orders levels so that a larger number is more verbose.
fn rank(level: Level) -> u8 {
    match level {
        Level::ERROR => 1,
        Level::WARN => 2,
        Level::INFO => 3,
        Level::DEBUG => 4,
        Level::TRACE => 5,
    }
}

/// Writes an event's fields as ` name=value`.
struct Fields(String);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }
}

impl Subscriber for Traced {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        rank(*metadata.level()) <= self.level.load(Ordering::Relaxed)
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &SpanRecord<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut text = Fields(String::new());
        event.record(&mut text);
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!(
                "{} {}: {}",
                event.metadata().level(),
                event.metadata().target(),
                text.0
            ));
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// Runs the app whose answer is lost, with the logger selected or not.
fn lose_an_answer(select: bool) -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let setup = RustStream::new(AppInfo::new("asks", "0.1.0"));
        let setup = if select {
            setup.logger(TracingLogger)
        } else {
            setup
        };
        let app = setup.with_broker(TightBus::new(), |b| {
            b.include(on_answers_stuck)
                .include(on_ask)
                .out_reply(LocalMemoryPublish)
        });
        let tb = TestApp::start(app).await?;
        // The first message fills the one slot of the answers subscription for the hour.
        for (destination, body) in [("answers", b"x"), ("ask", b"?")] {
            tb.broker::<TightBus>()
                .message(&Sent(body))
                .to(destination)
                .publish()
                .await?;
        }
        tb.advance(Duration::from_secs(1)).await?;
        tb.advance(Duration::from_secs(3600)).await?;
        tb.shutdown().await?;
        Ok::<_, Box<dyn Error>>(())
    })
}

/// Asserts the one warning is the lost answer, with its subscription and message type.
fn assert_lost_answer(lines: &[String]) {
    // The lifecycle events (info and debug) arrive too; the warnings are the subject.
    let warnings: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("WARN"))
        .collect();
    assert_eq!(warnings.len(), 1, "{lines:?}");
    assert!(
        warnings[0].starts_with("WARN ruststream::dispatch:"),
        "{lines:?}"
    );
    assert!(warnings[0].contains("message lost"), "{lines:?}");
    assert!(warnings[0].contains("subscription=ask"), "{lines:?}");
    assert!(warnings[0].contains("compio_logger::Verdict"), "{lines:?}");
}

#[test]
fn the_compio_runner_reports_only_through_the_logger_the_app_selected() -> Result<(), Box<dyn Error>>
{
    // No logger selected: the subscriber hears nothing.
    let silent = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(silent.clone());
        lose_an_answer(false)?;
    }
    assert!(silent.lines().is_empty(), "{:?}", silent.lines());

    // `TracingLogger`: the subscriber hears the warning with its context.
    let traced = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(traced.clone());
        lose_an_answer(true)?;
    }
    assert_lost_answer(&traced.lines());

    // The subscriber's level silences the warning, and the same event shows at the next level.
    let filtered = Traced::at(Level::ERROR);
    {
        let _scope = set_default(filtered.clone());
        lose_an_answer(true)?;
    }
    assert!(filtered.lines().is_empty(), "{:?}", filtered.lines());
    filtered.level.store(rank(Level::WARN), Ordering::Relaxed);
    {
        let _scope = set_default(filtered.clone());
        lose_an_answer(true)?;
    }
    assert_lost_answer(&filtered.lines());
    Ok(())
}
