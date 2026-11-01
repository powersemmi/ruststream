//! The `.error` continuation on the embedded runner: what a handler learns when a copy the
//! runtime publishes for its outcome is lost, whatever the cause. Driven through `TestApp` on
//! embassy-time's mock clock.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// A bus whose subscription holds one message, waiting or in hand.
type TightBus = LocalMemoryBroker<1>;

/// A bus with room for a few messages per subscription.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retries for.
const LATER: Duration = Duration::from_secs(5);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// What one `.error` continuation was told: the error, where the copy was going, its bytes.
type Told = (String, String, Vec<u8>);

/// What the `.error` continuations were told.
#[derive(Clone, Default)]
struct Lost(Arc<Mutex<Vec<Told>>>);

impl Lost {
    fn told(&self) -> Vec<Told> {
        self.0.lock().expect("the handlers never panic").clone()
    }
}

/// Asks for a retry later on every delivery of a job that is not `b`, and records what it learns
/// if the copy is lost. `b` holds its delivery for an hour of the mock clock.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, ctx: &mut Context<'_, (), Lost>) -> HandlerOutcome {
    if job.0 == b"b" {
        Timer::after(EmbassyDuration::from_secs(3600)).await;
        return HandlerOutcome::ack();
    }
    let lost = ctx.state().clone();
    HandlerOutcome::retry_after(LATER).error(async move |error, message| {
        lost.0.lock().expect("the handlers never panic").push((
            error.to_string(),
            message.destination().to_owned(),
            message.payload().to_vec(),
        ));
    })
}

/// The same retry with no continuation to tell.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_unwatched_job(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry_after(LATER)
}

/// Holds its delivery for an hour of the mock clock, so the bus stays full of it.
#[subscriber(LocalMemorySource::new("jobs.dead"))]
async fn on_stuck(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    Timer::after(EmbassyDuration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn one_record_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[ruststream::app(runtime = embassy)]
fn unwatched_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_unwatched_job).retry_capacity::<1>())
}

#[ruststream::app(runtime = embassy)]
fn tight_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[ruststream::app(runtime = embassy)]
fn dead_letter_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.include(on_stuck)
                .include(on_job)
                .max_attempts(nonzero!(2u32))
                .dead_letter("jobs.dead")
                .retry_capacity::<1>()
        })
}

/// Publishes the named jobs to `jobs`, in order.
macro_rules! publish_jobs {
    ($tb:expr, $($job:expr),+) => {
        for job in [$($job),+] {
            $tb.broker::<Bus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }
    };
}

const NO_FREE_RECORD: &str = "no free record in the store of retry copies (capacity 1)";

#[test]
fn a_copy_that_finds_no_free_record_reaches_the_error_continuation() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(one_record_app(Bus::new()), move |_| shared).await?;
        publish_jobs!(tb, b"one", b"two");

        tb.advance(Duration::from_secs(1)).await?;

        // The second job found the one record taken: its copy is lost, the handler is told,
        // and neither job is handled again before the first copy falls due.
        assert_eq!(
            lost.told(),
            [(
                NO_FREE_RECORD.to_owned(),
                "jobs".to_owned(),
                b"two".to_vec()
            )]
        );
        tb.broker::<Bus>().subscriber("jobs").assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_copy_that_finds_no_free_record_without_a_continuation_is_lost_without_a_rerun()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(unwatched_app(Bus::new())).await?;
        publish_jobs!(tb, b"one", b"two");

        tb.advance(Duration::from_secs(1)).await?;

        // No panic and no requeue: the second job ran once and its copy is gone.
        tb.broker::<Bus>().subscriber("jobs").assert_called(2);
        tb.advance(LATER).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(3);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_dead_letter_the_bus_refuses_reaches_the_error_continuation() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(dead_letter_app(TightBus::new()), move |_| shared).await?;
        // The dead-letter subscription holds this message for the hour, so it has no room.
        tb.broker::<TightBus>()
            .message(&Sent(b"x"))
            .to("jobs.dead")
            .publish()
            .await?;
        tb.broker::<TightBus>()
            .message(&Sent(b"a"))
            .to("jobs")
            .publish()
            .await?;

        // The first attempt is kept for its copy; the copy is the second attempt, which is the
        // cap, so its retry goes to the dead letter, and the bus refuses it.
        tb.advance(LATER).await?;
        tb.advance(Duration::from_secs(1)).await?;

        let told = lost.told();
        assert_eq!(told.len(), 1, "{told:?}");
        assert_eq!(told[0].1, "jobs.dead");
        assert_eq!(told[0].2, b"a".to_vec());
        tb.broker::<TightBus>().subscriber("jobs").assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_copy_the_bus_refuses_when_it_falls_due_reaches_the_error_continuation()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(tight_app(TightBus::new()), move |_| shared).await?;
        for job in [b"a", b"b"] {
            tb.broker::<TightBus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }

        // `b` holds the subscription's one slot for the hour, so `a`'s copy has no room when it
        // falls due.
        tb.advance(LATER).await?;

        let told = lost.told();
        assert_eq!(told.len(), 1, "{told:?}");
        assert_eq!(told[0].1, "jobs");
        assert_eq!(told[0].2, b"a".to_vec());
        tb.shutdown().await?;
        Ok(())
    })
}

/// What the `.then` and `.error` continuations of the pair handlers recorded, in order.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn told(&self) -> Vec<String> {
        self.0.lock().expect("the handlers never panic").clone()
    }

    fn push(&self, line: String) {
        self.0.lock().expect("the handlers never panic").push(line);
    }
}

/// The first delivery of a job asks for a retry and registers both continuations on its outcome,
/// `.and_after(..)` first; the copy that comes back is acknowledged.
#[subscriber(LocalMemorySource::new("outcome-then-error"))]
async fn outcome_then_error(job: &Job<'_>, ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    HandlerOutcome::retry_after(LATER)
        .and_after(async move { then.push("then".to_owned()) })
        .error(async move |err, lost| error.push(format!("error {err} {}", lost.destination())))
}

/// The same pair on the outcome, `.error(..)` first.
#[subscriber(LocalMemorySource::new("outcome-error-then"))]
async fn outcome_error_then(job: &Job<'_>, ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    HandlerOutcome::retry_after(LATER)
        .error(async move |err, lost| error.push(format!("error {err} {}", lost.destination())))
        .and_after(async move { then.push("then".to_owned()) })
}

/// The pair from the context, `.then(..)` first.
#[subscriber(LocalMemorySource::new("context-then-error"))]
async fn context_then_error(job: &Job<'_>, ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    ctx.after(HandlerOutcome::retry_after(LATER))
        .then(async move { then.push("then".to_owned()) })
        .error(async move |err, lost| error.push(format!("error {err} {}", lost.destination())));
    HandlerOutcome::retry_after(LATER)
}

/// The pair from the context, `.error(..)` first.
#[subscriber(LocalMemorySource::new("context-error-then"))]
async fn context_error_then(job: &Job<'_>, ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let (then, error) = (ctx.state().clone(), ctx.state().clone());
    ctx.after(HandlerOutcome::retry_after(LATER))
        .error(async move |err, lost| error.push(format!("error {err} {}", lost.destination())))
        .then(async move { then.push("then".to_owned()) });
    HandlerOutcome::retry_after(LATER)
}

#[ruststream::app(runtime = embassy)]
fn pairs_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Log>> {
    EmbeddedApp::new()
        .with_state(Log::default())
        .with_broker(bus, |b| {
            b.include(outcome_then_error)
                .retry_capacity::<1>()
                .include(outcome_error_then)
                .retry_capacity::<1>()
                .include(context_then_error)
                .retry_capacity::<1>()
                .include(context_error_then)
                .retry_capacity::<1>()
        })
}

const PAIRS: [&str; 4] = [
    "outcome-then-error",
    "outcome-error-then",
    "context-then-error",
    "context-error-then",
];

fn job_to(subject: &'static str) -> Sent {
    Sent(match subject {
        "outcome-then-error" => b"1",
        "outcome-error-then" => b"2",
        "context-then-error" => b"3",
        _ => b"4",
    })
}

#[test]
fn a_published_copy_runs_only_then_in_every_form_of_the_pair() -> Result<(), Box<dyn Error>> {
    for subject in PAIRS {
        block_on(async {
            let log = Log::default();
            let shared = log.clone();
            let tb = TestApp::with_state(pairs_app(Bus::new()), move |_| shared).await?;
            tb.broker::<Bus>()
                .message(&job_to(subject))
                .to(subject)
                .publish()
                .await?;
            tb.advance(LATER).await?;

            assert_eq!(log.told(), ["then"], "{subject}");
            tb.broker::<Bus>().subscriber(subject).assert_called(2);
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        })?;
    }
    Ok(())
}

#[test]
fn a_copy_without_a_record_runs_only_error_in_every_form_of_the_pair() -> Result<(), Box<dyn Error>>
{
    for subject in PAIRS {
        block_on(async {
            let log = Log::default();
            let shared = log.clone();
            let tb = TestApp::with_state(pairs_app(Bus::new()), move |_| shared).await?;
            for _ in 0..2 {
                tb.broker::<Bus>()
                    .message(&job_to(subject))
                    .to(subject)
                    .publish()
                    .await?;
            }
            tb.advance(Duration::from_secs(1)).await?;

            // The second job found the one record taken: only its `.error` runs, once; the
            // `.then` of the first job waits for its copy.
            assert_eq!(
                log.told(),
                [format!(
                    "error {} {subject}",
                    "no free record in the store of retry copies (capacity 1)"
                )],
                "{subject}"
            );
            tb.advance(LATER).await?;
            assert_eq!(log.told().len(), 2, "{subject}: the first copy's `.then`");
            assert_eq!(log.told()[1], "then", "{subject}");
            tb.shutdown().await?;
            Ok::<_, Box<dyn Error>>(())
        })?;
    }
    Ok(())
}

/// A continuation on the embedded runner may hold a value that is not `Send`: an `Rc` here, where
/// the same closure is refused on the tokio runner.
#[subscriber(LocalMemorySource::new("rc-job"))]
async fn rc_job(job: &Job<'_>, ctx: &mut Context<'_, (), Log>) -> HandlerOutcome {
    let _ = job.0;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let log = Rc::new(ctx.state().clone());
    let told = Rc::clone(&log);
    HandlerOutcome::retry_after(LATER)
        .error(async move |_err, _lost| told.push("error".to_owned()))
        .and_after(async move { log.push("then".to_owned()) })
}

#[ruststream::app(runtime = embassy)]
fn rc_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Log>> {
    EmbeddedApp::new()
        .with_state(Log::default())
        .with_broker(bus, |b| b.include(rc_job).retry_capacity::<1>())
}

#[test]
fn an_error_continuation_may_hold_a_value_that_is_not_send() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let log = Log::default();
        let shared = log.clone();
        let tb = TestApp::with_state(rc_app(Bus::new()), move |_| shared).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"1"))
            .to("rc-job")
            .publish()
            .await?;
        tb.advance(LATER).await?;

        assert_eq!(log.told(), ["then"]);
        tb.shutdown().await?;
        Ok(())
    })
}
