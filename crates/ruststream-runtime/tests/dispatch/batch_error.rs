//! The `.error` continuation on a batch: each element's outcome is covered like a single
//! delivery's, so one element's lost copy runs its own `.error` and leaves the others' `.then`.

#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use std::convert::Infallible;
use std::future::ready;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::join_all;
use ruststream::Buffered;
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, LogMode, MemoryPublish};
use ruststream::testing::TestApp;
use ruststream::{Lend, OutgoingMessage, PairError};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::error::{DownPublish, Job, Queue, RETRY_DELAY};
use ruststream_core::{PublishMessageError, PublishOutcome};

/// Publishes one job to `to`.
async fn publish_job<State: Send + Sync + 'static>(tb: &TestApp<State>, id: u32, to: &'static str) {
    tb.broker::<MemoryBroker>()
        .message(&Job { id })
        .to(to)
        .publish()
        .await
        .expect("publish");
}

type Log = Arc<Mutex<Vec<String>>>;

/// A publisher whose broker refuses the copy of job 1 and takes every other.
#[derive(Debug, Clone, Copy)]
struct Refuses1;

impl Publisher for Refuses1 {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        let refused = msg.payload().windows(7).any(|w| w == br#""id":1}"#);
        ready(if refused {
            Err(PublishMessageError::with_message(
                io::Error::other("refused"),
                msg,
            ))
        } else {
            Ok(())
        })
    }
}

/// A publisher whose broker refuses the copies of jobs 1 and 2 and takes every other.
#[derive(Debug, Clone, Copy)]
struct Refuses12;

impl Publisher for Refuses12 {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        let refused = [br#""id":1}"#, br#""id":2}"#]
            .iter()
            .any(|id| msg.payload().windows(7).any(|w| w == *id));
        ready(if refused {
            Err(PublishMessageError::with_message(
                io::Error::other("refused"),
                msg,
            ))
        } else {
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Refuses12Publish;

impl<L: LogMode> PublishPolicy<ConnectedMemoryBroker<L>> for Refuses12Publish {
    type Live = Refuses12;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<L>,
    ) -> impl Future<Output = Result<Refuses12, PairError>> {
        ready(Ok(Refuses12))
    }
}

#[derive(Debug, Clone, Copy)]
struct Refuses1Publish;

impl<L: LogMode> PublishPolicy<ConnectedMemoryBroker<L>> for Refuses1Publish {
    type Live = Refuses1;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<L>,
    ) -> impl Future<Output = Result<Refuses1, PairError>> {
        ready(Ok(Refuses1))
    }
}

/// A publisher whose broker refuses the copy of one job and takes every other.
#[derive(Debug, Clone, Copy)]
struct RefusesJob(u32);

impl Publisher for RefusesJob {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        let wanted = format!(r#""id":{}}}"#, self.0);
        let refused = msg
            .payload()
            .windows(wanted.len())
            .any(|w| w == wanted.as_bytes());
        ready(if refused {
            Err(PublishMessageError::with_message(
                io::Error::other("refused"),
                msg,
            ))
        } else {
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct RefusesJobPublish(u32);

impl<L: LogMode> PublishPolicy<ConnectedMemoryBroker<L>> for RefusesJobPublish {
    type Live = RefusesJob;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<L>,
    ) -> impl Future<Output = Result<RefusesJob, PairError>> {
        ready(Ok(RefusesJob(self.0)))
    }
}

/// Element 1 asks for a retry and watches its copy; element 2 is acknowledged and has a `.then`;
/// element 3 asks for a retry and watches its copy too, and the broker takes it.
type BatchedQueue = Buffered<Queue>;

#[subscriber(BatchedQueue::new(Queue { name: "per-element" }))]
async fn per_element(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> Vec<HandlerOutcome> {
    let log = ctx.state().clone();
    jobs.iter()
        .map(|job| {
            let id = job.id;
            let (then, error) = (Arc::clone(&log), Arc::clone(&log));
            let after = async move { then.lock().expect("log").push(format!("then {id}")) };
            if id == 2 {
                return HandlerOutcome::ack().and_after(after);
            }
            HandlerOutcome::retry_after(RETRY_DELAY)
                .and_after(after)
                .error(async move |failures: Failures| {
                    for failed in failures {
                        let (err, _lost) = failed.into_parts();
                        error.lock().expect("log").push(format!("error {id} {err}"));
                    }
                })
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_batch_element_with_a_lost_copy_runs_only_its_own_error() {
    let log = Log::default();
    let startup = Arc::clone(&log);
    let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(per_element.batch(nonzero!(3)))
                .out_retry(Refuses1Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    // Published together, so they reach the subscription as one batch.
    let publishes = [
        publish_job(&tb, 1, "per-element"),
        publish_job(&tb, 2, "per-element"),
        publish_job(&tb, 3, "per-element"),
    ];
    join_all(publishes).await;
    tb.advance(RETRY_DELAY).await.expect("advance");

    let mut told = log.lock().expect("log").clone();
    told.sort();
    assert_eq!(told, ["error 1 refused", "then 2", "then 3"]);
}

type UniformQueue = Buffered<Queue>;

/// One outcome for the whole batch: its `.error` runs once with every failed copy, in the batch's
/// order, and its `.and_after` once when at least one copy was published.
#[subscriber(UniformQueue::new(Queue { name: "uniform" }).max_wait(Duration::from_secs(1)))]
async fn uniform(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    let count = jobs.len();
    HandlerOutcome::retry_after(RETRY_DELAY)
        .and_after(async move { then.lock().expect("log").push(format!("then {count}")) })
        .error(async move |failures: Failures| {
            let count = failures.len();
            let each: Vec<String> = failures
                .map(|failed| {
                    let payload =
                        String::from_utf8_lossy(failed.message().payload().expect("the bytes"))
                            .into_owned();
                    format!(" {payload} {}", failed.error())
                })
                .collect();
            let told = format!("error x{count}{}", each.concat());
            error.lock().expect("log").push(told);
        })
}

/// A log the `.error` of `owning` owns outright: it is not `Clone`, so only a one-shot `.error`
/// accepts it.
struct Unique(Log);

/// A uniform outcome whose `.error` owns a value that cannot be cloned.
#[subscriber(UniformQueue::new(Queue { name: "owning" }).max_wait(Duration::from_secs(1)))]
async fn owning(_jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let owned = Unique(ctx.state().clone());
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
        // Consuming the whole value makes the closure own it, so the closure is not `Clone`.
        let log = owned.0.clone();
        drop(owned);
        log.lock()
            .expect("log")
            .push(format!("error x{}", failures.len()));
    })
}

/// Starts a batch of three jobs on `$handler` (subscribed as `$name`) whose retry copies go
/// through `$refuses`, and gives the log before and after the delayed copies are published.
macro_rules! run_uniform {
    ($handler:ident, $name:literal, $refuses:expr) => {{
        let log = Log::default();
        let startup = Arc::clone(&log);
        let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(startup))
            .with_broker(MemoryBroker::new(), |b| {
                b.include($handler.batch(nonzero!(3))).out_retry($refuses)
            });
        let tb = TestApp::start(app).await.expect("harness start");
        let publishes = [
            publish_job(&tb, 1, $name),
            publish_job(&tb, 2, $name),
            publish_job(&tb, 3, $name),
        ];
        join_all(publishes).await;
        let before = log.lock().expect("log").clone();
        tb.advance(RETRY_DELAY).await.expect("advance");
        let after = log.lock().expect("log").clone();
        (before, after)
    }};
}

/// The log in a fixed order, for continuations that run beside each other.
fn sorted(mut log: Vec<String>) -> Vec<String> {
    log.sort();
    log
}

/// Two of three copies fail: the `.error` runs once, with both failures in the batch's order, and
/// the `.and_after` runs once because one copy was published.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_outcome_runs_its_error_once_with_every_failed_copy() {
    let (before, after) = run_uniform!(uniform, "uniform", Refuses12Publish);

    assert!(
        before.is_empty(),
        "nothing has failed before the copies are due: {before:?}"
    );
    // The two continuations run beside each other, so neither order is promised.
    assert_eq!(
        sorted(after),
        [r#"error x2 {"id":1} refused {"id":2} refused"#, "then 3"]
    );
}

/// The last copy fails alone: one failure in the call, and the `.and_after` still runs once.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_outcome_runs_its_and_after_once_when_only_the_last_copy_fails() {
    let (_, after) = run_uniform!(uniform, "uniform", RefusesJobPublish(3));

    assert_eq!(sorted(after), [r#"error x1 {"id":3} refused"#, "then 3"]);
}

/// Every copy fails: the `.error` runs once with all of them and the `.and_after` never runs.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_outcome_skips_its_and_after_when_every_copy_fails() {
    let (_, after) = run_uniform!(uniform, "uniform", DownPublish);

    assert_eq!(
        after,
        [r#"error x3 {"id":1} broker down {"id":2} broker down {"id":3} broker down"#]
    );
}

/// The one-shot closure of a uniform outcome owns a capture that is not `Clone`, and runs once.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_error_owns_a_capture_that_is_not_clone() {
    let (_, after) = run_uniform!(owning, "owning", DownPublish);

    assert_eq!(after, ["error x3"]);
}

/// What `receipted` answers each job with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u32,
}

/// Answers a whole batch with one receipt per job, and records what happens to the answers.
#[subscriber(UniformQueue::new(Queue { name: "receipted" }).max_wait(Duration::from_secs(1)), reply)]
async fn receipted(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> Vec<Receipt> {
    let error = ctx.state().clone();
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            let each: Vec<String> = failures
                .map(|failed| {
                    let payload =
                        String::from_utf8_lossy(failed.message().payload().expect("the bytes"))
                            .into_owned();
                    format!(
                        " {} {payload} {}",
                        failed.message().destination(),
                        failed.error()
                    )
                })
                .collect();
            error
                .lock()
                .expect("log")
                .push(format!("error{}", each.concat()));
        });
    jobs.iter().map(|job| Receipt { id: job.id }).collect()
}

/// A batch of replies the broker refuses is acknowledged, and the context's `.error` runs once
/// with every reply that was lost.
#[tokio::test(start_paused = true)]
async fn a_failed_batch_reply_runs_the_context_error_with_the_lost_replies() {
    let log = Log::default();
    let startup = Arc::clone(&log);
    let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(receipted.batch(nonzero!(2)))
                .out_reply(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    join_all([
        publish_job(&tb, 1, "receipted"),
        publish_job(&tb, 2, "receipted"),
    ])
    .await;
    tb.settle().await.expect("settle");

    assert_eq!(
        *log.lock().expect("log"),
        [r#"error receipts {"id":1} broker down receipts {"id":2} broker down"#]
    );
}

/// A uniform outcome whose `.and_after` takes an hour.
#[subscriber(UniformQueue::new(Queue { name: "stuck" }).max_wait(Duration::from_secs(1)))]
async fn stuck(_jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    HandlerOutcome::retry_after(RETRY_DELAY)
        .and_after(async move {
            sleep(Duration::from_secs(3600)).await;
            then.lock().expect("log").push("then".to_owned());
        })
        .error(async move |failures: Failures| {
            error
                .lock()
                .expect("log")
                .push(format!("error x{}", failures.len()));
        })
}

/// A uniform outcome whose `.and_after` panics.
#[subscriber(UniformQueue::new(Queue { name: "panicking" }).max_wait(Duration::from_secs(1)))]
async fn panicking(_jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let error = ctx.state().clone();
    HandlerOutcome::retry_after(RETRY_DELAY)
        .and_after(async { panic!("the follow-up failed") })
        .error(async move |failures: Failures| {
            error
                .lock()
                .expect("log")
                .push(format!("error x{}", failures.len()));
        })
}

/// The `.error` of a uniform outcome does not wait for its `.and_after`: a slow follow-up cannot
/// hold the failed copies back from it.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_error_runs_while_the_and_after_is_slow() {
    let (_, after) = run_uniform!(stuck, "stuck", Refuses12Publish);

    assert_eq!(after, ["error x2", "then"]);
}

/// A follow-up that panics cannot keep the failed copies from the `.error` either.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_error_runs_while_the_and_after_panics() {
    let (_, after) = run_uniform!(panicking, "panicking", Refuses12Publish);

    assert_eq!(after, ["error x2"]);
}

/// Answers a whole batch and leaves a `.then` that waits for the answers to be published.
#[subscriber(UniformQueue::new(Queue { name: "receipted-then" }).max_wait(Duration::from_secs(1)), reply)]
async fn receipted_then(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> Vec<Receipt> {
    let log = ctx.state().clone();
    ctx.after(HandlerOutcome::ack())
        .error(async move |_failures: Failures| {})
        .then(async move { log.lock().expect("log").push("then".to_owned()) });
    jobs.iter().map(|job| Receipt { id: job.id }).collect()
}

/// A batch of replies the broker takes runs the context's `.then`, once.
#[tokio::test(start_paused = true)]
async fn a_published_batch_reply_runs_the_context_then() {
    let log = Log::default();
    let startup = Arc::clone(&log);
    let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(receipted_then.batch(nonzero!(2)))
                .out_reply(MemoryPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    join_all([
        publish_job(&tb, 1, "receipted-then"),
        publish_job(&tb, 2, "receipted-then"),
    ])
    .await;
    tb.settle().await.expect("settle");

    assert_eq!(*log.lock().expect("log"), ["then"]);
}

/// Answers a whole batch with one receipt per job, and records which replies the `.error` was
/// told of.
#[subscriber(UniformQueue::new(Queue { name: "partial" }).max_wait(Duration::from_secs(1)), reply)]
async fn partial(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> Vec<Receipt> {
    let error = ctx.state().clone();
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            let each: Vec<String> = failures
                .map(|failed| {
                    let payload =
                        String::from_utf8_lossy(failed.message().payload().expect("the bytes"))
                            .into_owned();
                    format!(" {payload}")
                })
                .collect();
            error
                .lock()
                .expect("log")
                .push(format!("error{}", each.concat()));
        });
    jobs.iter().map(|job| Receipt { id: job.id }).collect()
}

/// The broker takes the first reply and refuses the second: the `.error` is told of the refused
/// reply and of the one never sent, not of the reply that went out.
#[tokio::test(start_paused = true)]
async fn a_batch_reply_that_fails_midway_reports_the_failed_and_the_unsent_replies() {
    let log = Log::default();
    let startup = Arc::clone(&log);
    let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(partial.batch(nonzero!(3)))
                .out_reply(RefusesJobPublish(2))
        });
    let tb = TestApp::start(app).await.expect("harness start");
    join_all([
        publish_job(&tb, 1, "partial"),
        publish_job(&tb, 2, "partial"),
        publish_job(&tb, 3, "partial"),
    ])
    .await;
    tb.settle().await.expect("settle");

    assert_eq!(*log.lock().expect("log"), [r#"error {"id":2} {"id":3}"#]);
}

/// Answers a whole batch with one receipt per job, and records the type of each lost reply's error.
#[subscriber(UniformQueue::new(Queue { name: "typed-cause" }).max_wait(Duration::from_secs(1)), reply)]
async fn typed_cause(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> Vec<Receipt> {
    let error = ctx.state().clone();
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            let each: Vec<String> = failures
                .map(|failed| {
                    let payload =
                        String::from_utf8_lossy(failed.message().payload().expect("the bytes"))
                            .into_owned();
                    let kind = failed
                        .error()
                        .downcast_ref::<io::Error>()
                        .map(io::Error::kind);
                    format!(" {payload} {kind:?}")
                })
                .collect();
            error
                .lock()
                .expect("log")
                .push(format!("error{}", each.concat()));
        });
    jobs.iter().map(|job| Receipt { id: job.id }).collect()
}

/// Every reply of a failed batch carries the broker's own error: the `.error` can read its type.
#[tokio::test(start_paused = true)]
async fn a_batch_reply_error_keeps_the_type_of_the_broker_error() {
    let log = Log::default();
    let startup = Arc::clone(&log);
    let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(typed_cause.batch(nonzero!(3)))
                .out_reply(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    join_all([
        publish_job(&tb, 1, "typed-cause"),
        publish_job(&tb, 2, "typed-cause"),
        publish_job(&tb, 3, "typed-cause"),
    ])
    .await;
    tb.settle().await.expect("settle");

    assert_eq!(
        *log.lock().expect("log"),
        [r#"error {"id":1} Some(Other) {"id":2} Some(Other) {"id":3} Some(Other)"#]
    );
}

/// A publisher whose broker refuses the reply of job 2 and takes every other.
#[derive(Debug, Clone, Copy)]
struct Refuses2;

impl Publisher for Refuses2 {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        let refused = msg.payload().windows(7).any(|w| w == br#""id":2}"#);
        ready(if refused {
            Err(PublishMessageError::with_message(
                io::Error::other("refused"),
                msg,
            ))
        } else {
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Refuses2Publish;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for Refuses2Publish {
    type Live = Refuses2;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Refuses2, PairError>> {
        ready(Ok(Refuses2))
    }
}

/// A batch whose second reply the broker refuses tells `.error` of the replies that did not go
/// out, the refused one as it was sent; the reply that left is not among them.
#[tokio::test(start_paused = true)]
async fn a_refused_batch_reply_reports_only_the_replies_that_did_not_go_out() {
    let log = Log::default();
    let startup = Arc::clone(&log);
    let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(receipted.batch(nonzero!(2)))
                .out_reply(Refuses2Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    join_all([
        publish_job(&tb, 1, "receipted"),
        publish_job(&tb, 2, "receipted"),
    ])
    .await;
    tb.settle().await.expect("settle");

    assert_eq!(
        *log.lock().expect("log"),
        [r#"error receipts {"id":2} refused"#]
    );
}
