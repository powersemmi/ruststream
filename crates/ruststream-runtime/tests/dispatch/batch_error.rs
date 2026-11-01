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
use ruststream::memory::{ConnectedMemoryBroker, LogMode};
use ruststream::testing::TestApp;
use ruststream::{Lend, OutgoingMessage, PairError};

use crate::error::{DownPublish, Job, Queue, RETRY_DELAY};

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

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), io::Error>> {
        let refused = msg.payload().windows(7).any(|w| w == br#""id":1}"#);
        ready(if refused {
            Err(io::Error::other("refused"))
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

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), io::Error>> {
        let refused = [br#""id":1}"#, br#""id":2}"#]
            .iter()
            .any(|id| msg.payload().windows(7).any(|w| w == *id));
        ready(if refused {
            Err(io::Error::other("refused"))
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
                .error(async move |err, _lost| {
                    error.lock().expect("log").push(format!("error {id} {err}"));
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

/// One outcome for the whole batch, with an `.error` and a `.then` on it. Each failed copy is
/// logged by the job it carried.
#[subscriber(UniformQueue::new(Queue { name: "uniform" }).max_wait(Duration::from_secs(1)))]
async fn uniform(jobs: &[Job], ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    let count = jobs.len();
    HandlerOutcome::retry_after(RETRY_DELAY)
        .and_after(async move { then.lock().expect("log").push(format!("then {count}")) })
        .error(async move |err, lost| {
            let payload = String::from_utf8_lossy(lost.payload()).into_owned();
            error
                .lock()
                .expect("log")
                .push(format!("error {payload} {err}"));
        })
}

/// Starts a batch of three jobs on `uniform` whose retry copies go through `$refuses`, and gives
/// the log before and after the delayed copies are published.
macro_rules! run_uniform {
    ($refuses:expr) => {{
        let log = Log::default();
        let startup = Arc::clone(&log);
        let app = RustStream::new(AppInfo::new("batch", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(startup))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(uniform.batch(nonzero!(3))).out_retry($refuses)
            });
        let tb = TestApp::start(app).await.expect("harness start");
        let publishes = [
            publish_job(&tb, 1, "uniform"),
            publish_job(&tb, 2, "uniform"),
            publish_job(&tb, 3, "uniform"),
        ];
        join_all(publishes).await;
        let before = log.lock().expect("log").clone();
        tb.advance(RETRY_DELAY).await.expect("advance");
        let mut after = log.lock().expect("log").clone();
        after.sort();
        (before, after)
    }};
}

/// The uniform outcome's `.error` runs for each copy that fails, with that copy's bytes and
/// error, once the delayed copy is due; the `.then` runs once, for the batch.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_outcome_runs_its_error_for_each_failed_copy() {
    let (before, after) = run_uniform!(Refuses12Publish);

    assert!(
        before.is_empty(),
        "nothing has failed before the copies are due: {before:?}"
    );
    assert_eq!(
        after,
        [
            r#"error {"id":1} refused"#,
            r#"error {"id":2} refused"#,
            "then 3",
        ]
    );
}

/// Every copy fails: the `.error` runs once per element and the `.then`, which follows a publish
/// that succeeded, never runs.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_outcome_runs_its_error_for_every_element_when_all_fail() {
    let (_, after) = run_uniform!(DownPublish);

    assert_eq!(
        after,
        [
            r#"error {"id":1} broker down"#,
            r#"error {"id":2} broker down"#,
            r#"error {"id":3} broker down"#,
        ]
    );
}
