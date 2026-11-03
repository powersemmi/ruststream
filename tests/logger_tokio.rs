//! The tokio runner reports through the logger the app selects and through nothing else: with no
//! logger a lost reply is silent, `TracingLogger` and `LogLogger` each deliver it with its
//! subscription and message type, and the level the user's backend filters at silences or shows it.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.
// The tokio harness's futures are not `Send`; the test drives them on its own thread.
#![allow(clippy::future_not_send)]
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "testing",
    feature = "log",
    feature = "tokio"
))]

use std::future::ready;
use std::sync::atomic::Ordering;

use log::LevelFilter;
use ruststream::PublishOutcome;
use ruststream::diagnostics::{LogLogger, TracingLogger};
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, PairError, Take};
use tracing::Level;
use tracing::subscriber::set_default;

mod logger_capture;
use logger_capture::{RECORDS, Traced, rank, take_records};

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

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, BytesMut>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = PublishOutcome<'msg, MemoryError, Take>> {
        let _ = (&self.inner, options, msg);
        ready(Err(MemoryError::ShutDown.into()))
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

/// No lines.
const NONE: [String; 0] = [];

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
    assert!(
        lines[0].starts_with("WARN ruststream::dispatch:"),
        "{lines:?}"
    );
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
    assert_eq!(take_records(), NONE);

    // `TracingLogger`: the tracing subscriber hears it, the log crate does not.
    {
        let _scope = set_default(traced.clone());
        lose_a_reply(Choice::Tracing).await;
    }
    assert_lost_reply(&traced.lines());
    assert_eq!(take_records(), NONE);

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
    assert_eq!(take_records(), NONE);
    log::set_max_level(LevelFilter::Warn);
    lose_a_reply(Choice::Log).await;
    assert_lost_reply(&take_records());
}
