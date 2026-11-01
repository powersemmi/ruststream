//! What the settlement of a delivery does with the continuations its handler registered, when
//! the outcome is one the runtime publishes no copy for: an ack or a drop.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

/// The job the handlers receive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
struct Job {
    id: u32,
}

/// What the continuations recorded, in order.
#[derive(Clone, Default, FromRef)]
struct Events {
    log: Arc<Mutex<Vec<String>>>,
}

impl Events {
    fn told(&self) -> Vec<String> {
        self.log.lock().expect("log").clone()
    }
}

/// Records `entry` in the log when the future runs.
fn note(log: &Arc<Mutex<Vec<String>>>, entry: &'static str) -> impl Future<Output = ()> + use<> {
    let log = Arc::clone(log);
    async move { log.lock().expect("log").push(entry.to_owned()) }
}

/// An `.error` continuation that records every failure it is told about.
macro_rules! failed {
    ($log:expr) => {{
        let log = Arc::clone($log);
        async move |failures: Failures| {
            for _ in failures {
                log.lock().expect("log").push("error".to_owned());
            }
        }
    }};
}

/// Acks with an `.error` and the `.and_after` that follows a publish.
#[subscriber("acked")]
async fn acked(job: &Job, State(log): State<Arc<Mutex<Vec<String>>>>) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::ack()
        .and_after(note(&log, "then"))
        .error(failed!(&log))
}

/// Drops the job with the same pair.
#[subscriber("dropped")]
async fn dropped(job: &Job, State(log): State<Arc<Mutex<Vec<String>>>>) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::drop()
        .and_after(note(&log, "then"))
        .error(failed!(&log))
}

/// Registers pairs on the context, and returns an outcome with an `.error` of its own.
#[subscriber("own-error")]
async fn own_error(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> HandlerOutcome {
    let _ = job.id;
    ctx.after(HandlerOutcome::ack())
        .error(failed!(&log))
        .then(note(&log, "context then"));
    HandlerOutcome::ack().error(failed!(&log))
}

/// Registers three pairs under an ack on the context: only the first one's `.error` is kept.
#[subscriber("three-pairs")]
async fn three_pairs(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> HandlerOutcome {
    let _ = job.id;
    ctx.after(HandlerOutcome::ack())
        .error(failed!(&log))
        .then(note(&log, "first"));
    ctx.after(HandlerOutcome::ack()).error(failed!(&log));
    ctx.after(HandlerOutcome::ack())
        .error(failed!(&log))
        .then(note(&log, "third"));
    HandlerOutcome::ack()
}

fn app(events: &Events) -> impl App {
    let events = events.clone();
    RustStream::new(AppInfo::new("settle", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(events))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(acked)
                .include(dropped)
                .include(own_error)
                .include(three_pairs)
        })
}

/// Publishes one job to `subject` and returns what the continuations recorded.
async fn run(subject: &'static str) -> Vec<String> {
    let events = Events::default();
    let tb = TestApp::start(app(&events)).await.expect("harness start");
    tb.broker::<MemoryBroker>()
        .message(&Job { id: 1 })
        .to(subject)
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    events.told()
}

/// A delivery that is acked has no publish to fail: the `.then` runs, the `.error` never.
#[tokio::test(start_paused = true)]
async fn an_acked_delivery_runs_the_then_and_not_the_error() {
    assert_eq!(run("acked").await, ["then"]);
}

/// A dropped delivery likewise.
#[tokio::test(start_paused = true)]
async fn a_dropped_delivery_runs_the_then_and_not_the_error() {
    assert_eq!(run("dropped").await, ["then"]);
}

/// The outcome's own `.error` wins over the ones the context registered, whose `.then` stays.
#[tokio::test(start_paused = true)]
async fn the_outcomes_error_replaces_the_context_errors_and_keeps_their_then() {
    assert_eq!(run("own-error").await, ["context then"]);
}

/// Of several pairs registered for one outcome, the first keeps its `.error`; every pair keeps
/// its `.then`.
#[tokio::test(start_paused = true)]
async fn every_pair_of_the_outcome_keeps_its_then() {
    let mut told = run("three-pairs").await;
    told.sort();
    assert_eq!(told, ["first", "third"]);
}
