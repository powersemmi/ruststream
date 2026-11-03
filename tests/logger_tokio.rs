//! The tokio runner reports through the logger the app selects and through nothing else: with no
//! logger a lost reply is silent, `TracingLogger` and `LogLogger` each deliver it with its
//! subscription and message type, and the level the user's backend filters at silences or shows it.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "testing",
    feature = "log",
    feature = "tokio"
))]

use std::fmt::Write as _;
use std::future::ready;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use log::{LevelFilter, Log, Metadata, Record};
use ruststream::PublishOutcome;
use ruststream::diagnostics::{LogLogger, TracingLogger};
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, PairError, Take};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::subscriber::set_default;
use tracing::{Event, Level, Metadata as TracingMetadata, Subscriber};

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// The job's answer.
#[derive(Outgoing, Serialized)]
struct Answer(Vec<u8>);

/// A job for the app, as its bytes.
#[derive(Outgoing, Serialized)]
struct Work(Vec<u8>);

/// Answers every job; the answer's publisher refuses it.
#[subscriber("jobs", reply("answers"))]
async fn on_job(job: &Job<'_>) -> Answer {
    Answer(job.0.to_vec())
}

/// A reply policy whose publisher fails every publish, so the answer is lost.
struct Refusing;

#[derive(Clone)]
struct Refuser {
    inner: MemoryPublisher,
}

impl Publisher for Refuser {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    async fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, BytesMut>,
        options: Option<&Self::Options>,
    ) -> PublishOutcome<'msg, MemoryError, Take> {
        let _ = (&self.inner, options);
        let _ = msg;
        Err(MemoryError::ShutDown.into())
    }
}

impl PublishPolicy<ConnectedMemoryBroker> for Refusing {
    type Live = Refuser;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Refuser, PairError>> {
        ready(Ok(Refuser {
            inner: connected.publisher(),
        }))
    }
}

/// The `tracing` events of the framework a scope saw, filtered at `level`.
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
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }
}

impl Subscriber for Traced {
    fn enabled(&self, metadata: &TracingMetadata<'_>) -> bool {
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

/// The `log` records of the process, as text.
struct Records(Mutex<Vec<String>>);

impl Log for Records {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
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

static RECORDS: Records = Records(Mutex::new(Vec::new()));

/// The records written since the last call.
fn take_records() -> Vec<String> {
    std::mem::take(&mut *RECORDS.0.lock().unwrap_or_else(PoisonError::into_inner))
}

/// The logger an app selects.
#[derive(Clone, Copy)]
enum Choice {
    Nothing,
    Tracing,
    Log,
}

/// Runs the app whose reply is lost, with `choice` selecting the logger.
async fn lose_a_reply(choice: Choice) {
    let info = AppInfo::new("jobs", "0.1.0");
    let setup = RustStream::new(info);
    let setup = match choice {
        Choice::Nothing => setup,
        Choice::Tracing => setup.logger(TracingLogger),
        Choice::Log => setup.logger(LogLogger),
    };
    let app = setup.with_broker(MemoryBroker::new(), |b| {
        b.include(on_job).out_reply(Refusing)
    });
    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Work(b"x".to_vec()))
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.shutdown().await.expect("shutdown");
}

/// Asserts the one event is the lost reply, with its subscription and message type.
fn assert_lost_reply(lines: &[String]) {
    // The lifecycle events (info and debug) arrive too; the warnings are the subject.
    let lines: Vec<String> = lines
        .iter()
        .filter(|line| line.starts_with("WARN"))
        .cloned()
        .collect();
    let lines = lines.as_slice();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].starts_with("WARN ruststream::dispatch:"), "{lines:?}");
    assert!(lines[0].contains("message lost"), "{lines:?}");
    assert!(lines[0].contains("subscription=jobs"), "{lines:?}");
    assert!(lines[0].contains("message_type="), "{lines:?}");
    assert!(lines[0].contains("logger_tokio::Answer"), "{lines:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn the_framework_reports_only_through_the_logger_the_app_selected() {
    log::set_logger(&RECORDS).expect("no other logger in this binary");
    log::set_max_level(LevelFilter::Debug);

    // No logger selected: neither backend hears anything.
    let traced = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(traced.clone());
        lose_a_reply(Choice::Nothing).await;
    }
    assert!(traced.lines().is_empty(), "{:?}", traced.lines());
    assert!(take_records().is_empty());

    // `TracingLogger`: the tracing subscriber hears it, the log crate does not.
    {
        let _scope = set_default(traced.clone());
        lose_a_reply(Choice::Tracing).await;
    }
    assert_lost_reply(&traced.lines());
    assert!(take_records().is_empty());

    // The subscriber's level silences the warning, and the same event shows at the next level.
    let silenced = Traced::at(Level::ERROR);
    {
        let _scope = set_default(silenced.clone());
        lose_a_reply(Choice::Tracing).await;
    }
    assert!(silenced.lines().is_empty(), "{:?}", silenced.lines());
    silenced.level.store(rank(Level::WARN), Ordering::Relaxed);
    {
        let _scope = set_default(silenced.clone());
        lose_a_reply(Choice::Tracing).await;
    }
    assert_lost_reply(&silenced.lines());

    // `LogLogger`: the log crate hears it, the tracing subscriber does not.
    let quiet = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(quiet.clone());
        lose_a_reply(Choice::Log).await;
    }
    assert!(quiet.lines().is_empty(), "{:?}", quiet.lines());
    assert_lost_reply(&take_records());

    // `log::max_level()` silences the warning, and the same event shows at the next level.
    log::set_max_level(LevelFilter::Error);
    lose_a_reply(Choice::Log).await;
    assert!(take_records().is_empty());
    log::set_max_level(LevelFilter::Warn);
    lose_a_reply(Choice::Log).await;
    assert_lost_reply(&take_records());
}
