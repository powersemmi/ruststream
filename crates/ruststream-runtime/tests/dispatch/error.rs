//! The `.error` continuation of a `HandlerOutcome`: what a handler learns when a publish the
//! runtime makes for its outcome fails, and what happens to a failure nobody asked about.
//!
//! The broker is the in-memory one with its native delayed redelivery taken away, which puts the
//! runtime on the copy path, and with the copies' publisher swapped for one that always fails.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use std::convert::Infallible;
use std::future::ready;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, LogMode, MemoryPublisher};
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use tokio::time::sleep;

use crate::log_capture;
use ruststream::{
    AckError, AddressedCopies, BrokerMoves, Lend, OutgoingMessage, PairError, RedeliveryAddress,
    RedeliveryAddressed, Settle, Subscribe, Subscriber, SubscriptionSource, Take,
};
use ruststream_core::{PublishMessageError, PublishOutcome};
use serde::{Deserialize, Serialize};

pub(crate) const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The job the handlers receive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
pub(crate) struct Job {
    pub(crate) id: u32,
}

/// What the `.error` continuation sends to the alert subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
struct Alert {
    destination: String,
    error: String,
    bytes: usize,
}

/// What the handlers' `.error` continuations recorded, in the app's state.
#[derive(Clone, Default, FromRef)]
struct Lost {
    alerts: Arc<Mutex<Vec<Alert>>>,
}

/// A subscription of a broker that holds no delivery back for a delay: the runtime publishes the
/// copies, to the subscription's own name.
#[derive(Debug, Clone)]
pub(crate) struct Queue {
    pub(crate) name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for Queue
where
    C::Subscriber: Subscriber,
{
    type Subscriber = ShapedSubscriber<C::Subscriber>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(ShapedSubscriber(connected.subscribe(self.name).await?))
    }
}

impl<C: Subscribe> RedeliveryAddressed<C> for Queue
where
    C::Subscriber: Subscriber,
{
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

pub(crate) struct ShapedSubscriber<S>(S);

impl<S: Subscriber> Subscriber for ShapedSubscriber<S> {
    type Message = ShapedMessage<S::Message>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(ShapedMessage))
    }
}

/// A delivery that settles like the broker's own and keeps the trait defaults for its delivery
/// count and its delayed redelivery.
pub(crate) struct ShapedMessage<M>(M);

impl<M: IncomingMessage> IncomingMessage for ShapedMessage<M> {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }
}

impl<M: Settle> Settle for ShapedMessage<M> {
    async fn ack(self) -> Result<(), AckError> {
        self.0.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.0.nack(requeue).await
    }
}

/// A publisher whose broker is down: every publish fails, and it only reads the bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Down;

impl Publisher for Down {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        ready(Err(PublishMessageError::with_message(
            io::Error::other("broker down"),
            msg,
        )))
    }
}

/// The policy that pairs into [`Down`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct DownPublish;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for DownPublish {
    type Live = Down;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Down, PairError>> {
        ready(Ok(Down))
    }
}

/// Asks for a retry later, and records what it learns if the copy is lost.
#[subscriber(Queue { name: "jobs" })]
async fn on_job(job: &Job, State(alerts): State<Arc<Mutex<Vec<Alert>>>>) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
        for failed in failures {
            let (err, lost) = failed.into_parts();
            let alert = Alert {
                destination: lost.destination().to_owned(),
                error: err.to_string(),
                bytes: lost.payload().len(),
            };
            alerts.lock().expect("alerts").push(alert);
        }
    })
}

fn failing_app(lost: &Lost) -> impl App {
    let lost = lost.clone();
    RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(lost))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(on_job).out_retry(DownPublish)
        })
}

/// A retry copy the broker refuses reaches the handler's `.error` with the error and the message,
/// and the delivery is not run again.
#[tokio::test(start_paused = true)]
async fn a_failed_retry_copy_reaches_the_error_continuation() {
    let lost = Lost::default();
    let tb = TestApp::start(failing_app(&lost))
        .await
        .expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 7 })
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called_once();
    assert_eq!(
        *lost.alerts.lock().expect("alerts"),
        [Alert {
            destination: "jobs".to_owned(),
            error: "broker down".to_owned(),
            bytes: br#"{"id":7}"#.len(),
        }],
    );
}

/// What the `.then` and `.error` continuations of the pair handlers recorded, in order.
#[derive(Clone, Default, FromRef)]
struct Events {
    log: Arc<Mutex<Vec<String>>>,
}

impl Events {
    fn told(&self) -> Vec<String> {
        self.log.lock().expect("log").clone()
    }
}

/// The first delivery of a job asks for a retry and registers both continuations on its outcome,
/// `.and_after(..)` first; the copy that comes back is acknowledged.
#[subscriber(Queue { name: "outcome-then-error" })]
async fn outcome_then_error(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> HandlerOutcome {
    let _ = job.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let on_error = Arc::clone(&log);
    HandlerOutcome::retry_after(RETRY_DELAY)
        .and_after(async move { log.lock().expect("log").push("then".to_owned()) })
        .error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                on_error
                    .lock()
                    .expect("log")
                    .push(format!("error {err} {}", lost.destination()));
            }
        })
}

/// The same pair on the outcome, `.error(..)` first.
#[subscriber(Queue { name: "outcome-error-then" })]
async fn outcome_error_then(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> HandlerOutcome {
    let _ = job.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let on_error = Arc::clone(&log);
    HandlerOutcome::retry_after(RETRY_DELAY)
        .error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                on_error
                    .lock()
                    .expect("log")
                    .push(format!("error {err} {}", lost.destination()));
            }
        })
        .and_after(async move { log.lock().expect("log").push("then".to_owned()) })
}

/// The pair from the context, `.then(..)` first.
#[subscriber(Queue { name: "context-then-error" })]
async fn context_then_error(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> HandlerOutcome {
    let _ = job.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let on_error = Arc::clone(&log);
    ctx.after(HandlerOutcome::retry_after(RETRY_DELAY))
        .then(async move { log.lock().expect("log").push("then".to_owned()) })
        .error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                on_error
                    .lock()
                    .expect("log")
                    .push(format!("error {err} {}", lost.destination()));
            }
        });
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The pair from the context, `.error(..)` first.
#[subscriber(Queue { name: "context-error-then" })]
async fn context_error_then(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> HandlerOutcome {
    let _ = job.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        return HandlerOutcome::ack();
    }
    let on_error = Arc::clone(&log);
    ctx.after(HandlerOutcome::retry_after(RETRY_DELAY))
        .error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                on_error
                    .lock()
                    .expect("log")
                    .push(format!("error {err} {}", lost.destination()));
            }
        })
        .then(async move { log.lock().expect("log").push("then".to_owned()) });
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The four pair handlers over a broker whose retry copies reach the bus.
fn pairs_app(events: &Events) -> impl App {
    let events = events.clone();
    RustStream::new(AppInfo::new("pairs", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(events))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(outcome_then_error)
                .include(outcome_error_then)
                .include(context_then_error)
                .include(context_error_then)
        })
}

/// The four pair handlers over a broker that refuses every retry copy.
fn refusing_pairs_app(events: &Events) -> impl App {
    let events = events.clone();
    RustStream::new(AppInfo::new("pairs", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(events))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(outcome_then_error)
                .out_retry(DownPublish)
                .include(outcome_error_then)
                .out_retry(DownPublish)
                .include(context_then_error)
                .out_retry(DownPublish)
                .include(context_error_then)
                .out_retry(DownPublish)
        })
}

const PAIRS: [&str; 4] = [
    "outcome-then-error",
    "outcome-error-then",
    "context-then-error",
    "context-error-then",
];

/// A retry copy that reaches the bus runs the `.then` of the pair once and never the `.error`,
/// whichever way round the pair is written and from wherever it is registered.
#[tokio::test(start_paused = true)]
async fn a_published_copy_runs_only_then_in_every_form_of_the_pair() {
    for subject in PAIRS {
        let events = Events::default();
        let tb = TestApp::start(pairs_app(&events))
            .await
            .expect("harness start");
        tb.broker::<MemoryBroker>()
            .message(&Job { id: 1 })
            .to(subject)
            .publish()
            .await
            .expect("publish");
        tb.advance(RETRY_DELAY).await.expect("advance");

        assert_eq!(events.told(), ["then"], "{subject}");
        tb.broker::<MemoryBroker>()
            .subscriber(subject)
            .assert_called(2);
    }
}

/// A retry copy the broker refuses runs only the `.error` of the pair, once, with the error and
/// the message, and never the `.then`.
#[tokio::test(start_paused = true)]
async fn a_refused_copy_runs_only_error_in_every_form_of_the_pair() {
    for subject in PAIRS {
        let events = Events::default();
        let tb = TestApp::start(refusing_pairs_app(&events))
            .await
            .expect("harness start");
        tb.broker::<MemoryBroker>()
            .message(&Job { id: 1 })
            .to(subject)
            .publish()
            .await
            .expect("publish");
        tb.advance(RETRY_DELAY).await.expect("advance");

        assert_eq!(
            events.told(),
            [format!("error broker down {subject}")],
            "{subject}"
        );
        tb.broker::<MemoryBroker>()
            .subscriber(subject)
            .assert_called_once();
    }
}

/// A publisher whose bus takes everything but the dead letter, and keeps what it is handed.
#[derive(Clone)]
struct RefusesDeadLetter(MemoryPublisher);

impl Publisher for RefusesDeadLetter {
    type Payload = Take;
    type Error = io::Error;
    type Options = ();

    async fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, BytesMut>,
        options: Option<&()>,
    ) -> PublishOutcome<'msg, io::Error, Take> {
        if msg.name() == "jobs.dead" {
            return Err(PublishMessageError::with_message(
                io::Error::other("dead letter refused"),
                msg,
            ));
        }
        self.0
            .publish(msg, options)
            .await
            .map_err(|failed| PublishMessageError::new(io::Error::other(failed.into_error())))
    }
}

#[derive(Debug, Clone, Copy)]
struct RefusingDeadLetter;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for RefusingDeadLetter {
    type Live = RefusesDeadLetter;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RefusesDeadLetter, PairError>> {
        ready(Ok(RefusesDeadLetter(connected.publisher())))
    }
}

/// The dead-letter copy a retry becomes at the mount's cap is a publish the runtime makes for the
/// outcome: when it fails, the handler's `.error` is told, whether the publisher consumed the
/// bytes or only read them.
#[tokio::test(start_paused = true)]
async fn a_failed_dead_letter_copy_reaches_the_error_continuation() {
    let lost = Lost::default();
    let startup = lost.clone();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(on_job)
                .max_attempts(nonzero!(2u32))
                .dead_letter("jobs.dead")
                .out_retry(RefusingDeadLetter)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 3 })
        .to("jobs")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    // The first retry is a copy to the subscription; the second delivery is at the cap, so its
    // retry goes to the dead letter, which the bus refuses.
    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called(2);
    assert_eq!(
        *lost.alerts.lock().expect("alerts"),
        [Alert {
            destination: "jobs.dead".to_owned(),
            error: "dead letter refused".to_owned(),
            bytes: br#"{"id":3}"#.len(),
        }],
    );
}

/// What a one-shot `.error` owns: not `Clone`, so only a closure that is called once accepts it.
struct Ticket(Lost);

/// The `.error` of a single outcome is a one-shot closure: it may own a value that cannot be
/// cloned, and it runs once for the one copy that fails.
#[tokio::test(start_paused = true)]
async fn a_single_outcome_error_owns_a_capture_that_is_not_clone() {
    #[subscriber(Queue { name: "ticketed" })]
    async fn ticketed(job: &Job, ctx: &mut Context<'_, (), Lost>) -> HandlerOutcome {
        let ticket = Ticket(ctx.state().clone());
        let id = job.id;
        HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
            // Consuming the whole ticket makes the closure own it, so the closure is not `Clone`.
            let ticket = ticket;
            for failed in failures {
                let (err, lost) = failed.into_parts();
                // Consuming the whole ticket makes the closure own it, so the closure is not `Clone`.
                let lost_log = ticket.0.clone();
                lost_log.alerts.lock().expect("alerts").push(Alert {
                    destination: lost.destination().to_owned(),
                    error: format!("{id} {err}"),
                    bytes: lost.payload().len(),
                });
            }
        })
    }

    let lost = Lost::default();
    let startup = lost.clone();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ticketed).out_retry(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 7 })
        .to("ticketed")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    assert_eq!(lost.alerts.lock().expect("alerts").len(), 1);
}

/// A single delivery's `Failures` holds one `Failed`, with the error and the message the runtime
/// tried to send: its bytes and the headers it carried, the retry count among them.
#[tokio::test(start_paused = true)]
async fn a_single_delivery_hands_one_failed_with_its_bytes_and_headers() {
    #[subscriber(Queue { name: "single" })]
    async fn single(job: &Job, ctx: &mut Context<'_, (), Lost>) -> HandlerOutcome {
        let lost = ctx.state().clone();
        let id = job.id;
        HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
            assert_eq!(failures.len(), 1);
            for failed in failures {
                let retries = failed
                    .message()
                    .headers()
                    .get_str(RETRY_COUNT_HEADER)
                    .unwrap_or("none")
                    .to_owned();
                lost.alerts.lock().expect("alerts").push(Alert {
                    destination: failed.message().destination().to_owned(),
                    error: format!("{id} {} retries {retries}", failed.error()),
                    bytes: failed.message().payload().len(),
                });
            }
        })
    }

    let lost = Lost::default();
    let startup = lost.clone();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(single).out_retry(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 8 })
        .to("single")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    assert_eq!(
        *lost.alerts.lock().expect("alerts"),
        [Alert {
            destination: "single".to_owned(),
            error: "8 broker down retries 1".to_owned(),
            bytes: br#"{"id":8}"#.len(),
        }],
    );
}

/// Republishes the very message that failed through another `Out`: the same bytes reach another
/// destination, with no decoding in between.
#[subscriber(Queue { name: "forwarding" })]
async fn forwarding(job: &Job, Out(dead): Out<impl Publisher>) -> HandlerOutcome {
    let _ = job.id;
    let dead = dead.clone();
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
        for failed in failures {
            let message = failed.into_message();
            dead.message(&message)
                .to("dead")
                .with_headers(message.headers().clone())
                .publish()
                .await
                .expect("forwarded");
        }
    })
}

#[tokio::test(start_paused = true)]
async fn a_failed_message_is_republished_through_another_out_as_it_was() {
    let app =
        RustStream::new(AppInfo::new("errors", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(forwarding)
                .out(DefaultSlot, Publish)
                .out_retry(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 9 })
        .to("forwarding")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<MemoryBroker>()
        .published::<Job>("dead")
        .assert_called_once()
        .with(&Job { id: 9 });
}

/// A copy the broker refuses, with no `.error` to tell, is logged with the subscription, the
/// destination, the message type and the error, and is lost: the handler does not run again.
#[tokio::test(start_paused = true)]
async fn a_failed_copy_without_a_continuation_is_logged_and_lost() {
    #[subscriber(Queue { name: "unwatched" })]
    async fn unwatched(job: &Job) -> HandlerOutcome {
        let _ = job.id;
        HandlerOutcome::retry_after(RETRY_DELAY)
    }

    let captured = log_capture::Captured::default();
    let _installed = captured.install();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(unwatched).out_retry(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 5 })
        .to("unwatched")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<MemoryBroker>()
        .subscriber("unwatched")
        .assert_called_once();
    let lines = captured.lines_with("publishing the message failed; message lost");
    assert_eq!(lines.len(), 1, "{lines:?}");
    for needle in [
        "subscription=unwatched",
        "destination=unwatched",
        "error=broker down",
        "Job",
    ] {
        assert!(lines[0].contains(needle), "{needle}: {}", lines[0]);
    }
}

/// A subscription whose broker retries and dead-letters natively.
#[derive(Debug, Clone)]
struct ManagedQueue {
    name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for ManagedQueue
where
    C::Subscriber: Subscriber,
{
    type Subscriber = C::Subscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe(self.name).await
    }
}

/// Asks for a retry later, with an `.error` the runtime can never run on this subscription.
#[subscriber(ManagedQueue { name: "managed" })]
async fn managed_job(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
        for failed in failures {
            let (_err, _lost) = failed.into_parts();
        }
    })
}

/// Returns an outcome with no `.error`.
#[subscriber(ManagedQueue { name: "managed" })]
async fn managed_plain_job(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// Each outcome that carries an `.error` on a subscription whose broker moves the deliveries
/// warns, naming the subscription; the compile and the startup stay quiet.
#[tokio::test(start_paused = true)]
async fn an_error_continuation_on_a_broker_moved_subscription_warns_each_time() {
    let captured = log_capture::Captured::default();
    let _installed = captured.install();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(managed_job));
    let tb = TestApp::start(app).await.expect("harness start");
    let marker = "the outcome carries an `.error` continuation";
    assert!(captured.lines_with(marker).is_empty(), "nothing at startup");

    for id in [1, 2] {
        tb.broker::<MemoryBroker>()
            .message(&Job { id })
            .to("managed")
            .publish()
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let lines = captured.lines_with(marker);
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[0].contains("subscription=managed"), "{}", lines[0]);
}

/// An outcome with no `.error` on such a subscription costs no warning.
#[tokio::test(start_paused = true)]
async fn an_outcome_without_an_error_continuation_on_a_broker_moved_subscription_is_quiet() {
    let captured = log_capture::Captured::default();
    let _installed = captured.install();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(managed_plain_job));
    let tb = TestApp::start(app).await.expect("harness start");
    tb.broker::<MemoryBroker>()
        .message(&Job { id: 1 })
        .to("managed")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    assert_eq!(
        captured.lines_with("the outcome carries an `.error` continuation"),
        Vec::<String>::new()
    );
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
#[outgoing(name = "answers")]
struct Answer {
    id: u32,
}

/// Answers a job; if the answer cannot be published, the context's pair says what happened.
#[subscriber("ask", reply)]
async fn ask(job: &Job, ctx: &mut Context, State(log): State<Arc<Mutex<Vec<String>>>>) -> Answer {
    let on_error = Arc::clone(&log);
    ctx.after(HandlerOutcome::ack())
        .then(async move { log.lock().expect("log").push("then".to_owned()) })
        .error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                on_error
                    .lock()
                    .expect("log")
                    .push(format!("error {err} {}", lost.destination()));
            }
        });
    Answer { id: job.id }
}

/// A reply the broker refuses does not requeue the delivery: the handler runs once, the delivery
/// is acknowledged, and the context's `.error` runs with the reply that was lost.
#[tokio::test(start_paused = true)]
async fn a_failed_reply_runs_the_context_error_and_does_not_requeue() {
    let events = Events::default();
    let startup = events.clone();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ask).out_reply(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 4 })
        .to("ask")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("ask")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(events.told(), ["error broker down answers"]);
}

/// A reply that is published runs the pair's `.then` and not its `.error`.
#[tokio::test(start_paused = true)]
async fn a_published_reply_runs_the_context_then_and_not_the_error() {
    let events = Events::default();
    let startup = events.clone();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| b.include(ask));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 4 })
        .to("ask")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    assert_eq!(events.told(), ["then"]);
    tb.broker::<MemoryBroker>()
        .published::<Answer>("answers")
        .assert_called_once()
        .with(&Answer { id: 4 });
}

/// Asks for a retry later, and sends the alert through its own `Out` publisher if the copy is lost.
#[subscriber(Queue { name: "outs" })]
async fn on_job_out(job: &Job, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    let id = job.id;
    let alerts = alerts.clone();
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
        for failed in failures {
            let (err, lost) = failed.into_parts();
            let alert = Alert {
                destination: lost.destination().to_owned(),
                error: err.to_string(),
                bytes: usize::try_from(id).expect("small id"),
            };
            alerts
                .message(&alert)
                .to("alerts")
                .publish()
                .await
                .expect("alert published");
        }
    })
}

/// The `.error` closure publishes through a cloned `Out` publisher it captured.
#[tokio::test(start_paused = true)]
async fn the_error_continuation_publishes_through_a_captured_out_publisher() {
    let app =
        RustStream::new(AppInfo::new("errors", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(on_job_out)
                .out(DefaultSlot, Publish)
                .out_retry(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 7 })
        .to("outs")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<MemoryBroker>()
        .published::<Alert>("alerts")
        .assert_called_once()
        .with(&Alert {
            destination: "outs".to_owned(),
            error: "broker down".to_owned(),
            bytes: 7,
        });
}

/// Answers a job; the continuation for a lost answer takes a moment to finish.
#[subscriber("slow-ask", reply)]
async fn slow_ask(
    job: &Job,
    ctx: &mut Context,
    State(log): State<Arc<Mutex<Vec<String>>>>,
) -> Answer {
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                sleep(RETRY_DELAY).await;
                log.lock()
                    .expect("log")
                    .push(format!("error {err} {}", lost.destination()));
            }
        });
    Answer { id: job.id }
}

/// A reply that fails while the app shuts down has its `.error` run before shutdown returns, as
/// a `.then` does.
#[tokio::test(start_paused = true)]
async fn the_error_continuation_of_a_failed_reply_runs_before_shutdown_returns() {
    let events = Events::default();
    let startup = events.clone();
    let app = RustStream::new(AppInfo::new("errors", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(slow_ask).out_reply(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 4 })
        .to("slow-ask")
        .publish()
        .await
        .expect("publish");
    tb.shutdown().await.expect("shutdown");

    assert_eq!(events.told(), ["error broker down answers"]);
}
