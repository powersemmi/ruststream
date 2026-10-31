use std::future;
use std::future::ready;
use std::mem;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use ruststream::memory::{MemoryBroker, MemoryPublish};

use super::fixtures::{PlainMessage, Settlement, Unscoped};
use super::*;
use crate::testkit::log_capture;
use crate::{
    CallerName, Connected, Handle, Name, OutgoingDestination, Router, SubscriberBuilder,
    SubscriberSettings, nonzero, subscriber,
};
use crate::{Context, FailurePolicies, HandlerOutcome, HeaderMap, SendHost, SubscriptionEnd};

#[derive(Debug, thiserror::Error)]
#[error("subscriber stream failed")]
struct StreamFault;

/// Replays a fixed script of stream items, so a test can put a delivery behind a stream error.
struct ScriptedSubscriber {
    items: Vec<Result<PlainMessage, StreamFault>>,
}

impl Subscriber for ScriptedSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::iter(mem::take(&mut self.items))
    }
}

/// A backlog long enough that a loop reaching the end of it has plainly drained the
/// subscription rather than stopped on the shutdown signal.
const BACKLOG: usize = 10_000;

/// One delivery of the backlog, carrying its own position: what a handler reports, so a test can
/// tell a delivery the loop lost from one it handled twice.
fn numbered(position: usize) -> PlainMessage {
    PlainMessage {
        payload: Bytes::copy_from_slice(&position.to_be_bytes()),
        headers: HeaderMap::new(),
        settlement: Settlement::Accepted,
    }
}

/// The position [`numbered`] wrote into a delivery.
fn position_of(payload: &Bytes) -> usize {
    usize::from_be_bytes(
        payload
            .as_ref()
            .try_into()
            .expect("a delivery of the numbered backlog"),
    )
}

/// The batch form of a subscriber's own stream. A batch closes as soon as it is full, so the
/// size stays at one: a subscription that goes quiet hands over the delivery it has rather than
/// waiting for the rest of the batch.
fn batched<S>(
    stream: S,
    size: NonZeroUsize,
) -> impl Stream<Item = Result<Vec<PlainMessage>, StreamFault>> + Send
where
    S: Stream<Item = Result<PlainMessage, StreamFault>> + Send,
{
    stream
        .chunks(size.get())
        .map(|chunk| chunk.into_iter().collect())
}

/// A subscriber with a long backlog, every delivery ready on the first poll: the loop never
/// waits on it, so the shutdown signal is the only thing that can stop it early.
struct SaturatedSubscriber;

impl Subscriber for SaturatedSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::iter((0..BACKLOG).map(|position| Ok(numbered(position))))
    }
}

impl BatchSubscriber for SaturatedSubscriber {
    type Batch = Vec<PlainMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        batched(self.stream(), size)
    }
}

/// A subscriber with one delivery and then nothing: the loop is parked on an empty subscription
/// by the time the test signals shutdown.
struct QuietSubscriber;

impl Subscriber for QuietSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::once(ready(Ok(numbered(0)))).chain(stream::pending())
    }
}

impl BatchSubscriber for QuietSubscriber {
    type Batch = Vec<PlainMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        batched(self.stream(), size)
    }
}

/// Reports every delivery it handled, so the test can await progress instead of sleeping.
struct ReportingHandler {
    seen: mpsc::UnboundedSender<Bytes>,
}

impl Handler<PlainMessage, (), ()> for ReportingHandler {
    fn handle(
        &self,
        msg: &PlainMessage,
        _ctx: &mut Context<'_, (), ()>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        let sent = self.seen.send(msg.payload.clone());
        async move {
            sent.expect("the test holds the receiver");
            HandlerOutcome::ack()
        }
    }
}

fn scripted(payloads: &[&'static str]) -> ScriptedSubscriber {
    // The fault comes first so the loop has to survive it to reach any delivery.
    let mut items: Vec<Result<PlainMessage, StreamFault>> = vec![Err(StreamFault)];
    items.extend(payloads.iter().map(|payload| {
        Ok(PlainMessage {
            payload: Bytes::from_static(payload.as_bytes()),
            headers: HeaderMap::new(),
            settlement: Settlement::Accepted,
        })
    }));
    ScriptedSubscriber { items }
}

fn dispatch_failure() -> DispatchFailure<Shutdown> {
    DispatchFailure::new(FailurePolicies::default(), Shutdown::new())
}

/// Drives one scripted subscriber through `workers` and returns the payloads that reached the
/// handler, in arrival order.
async fn dispatched_under(workers: Workers, payloads: &[&'static str]) -> Vec<Bytes> {
    let (seen, mut arrived) = mpsc::unbounded_channel();
    let joined = spawn_dispatch_workers(
        scripted(payloads),
        Arc::new(ReportingHandler { seen }),
        Shutdown::new(),
        Arc::from("orders"),
        Arc::new(()),
        Arc::new(Hosted::empty()),
        dispatch_failure(),
        workers,
    )
    .expect("the dispatch starts");

    let mut handled = Vec::with_capacity(payloads.len());
    for _ in payloads {
        handled.push(arrived.recv().await.expect("delivery should be handled"));
    }
    // The script ends, so the loop terminates on its own rather than on shutdown.
    joined.await.expect("dispatch task should not panic");
    handled
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_error_does_not_stop_the_sequential_loop() {
    let handled = dispatched_under(Workers::sequential(), &["first", "second"]).await;
    assert_eq!(
        handled,
        vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_error_does_not_stop_the_worker_pool() {
    let handled = dispatched_under(Workers::pool(NonZeroUsize::new(2).unwrap()), &["a", "b"]).await;
    // The pool loses global order by design, so assert the set, not the sequence.
    let mut handled = handled;
    handled.sort();
    assert_eq!(
        handled,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_error_does_not_stop_the_keyed_lanes() {
    // Keyless deliveries rotate over the lanes, so both lanes get exercised.
    let handled =
        dispatched_under(Workers::keyed(NonZeroUsize::new(2).unwrap()), &["a", "b"]).await;
    let mut handled = handled;
    handled.sort();
    assert_eq!(
        handled,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}

/// Reports every delivery of a batch and settles it, the way the batch adapters do: a batch
/// handler owns the deliveries it was handed.
struct ReportingBatchHandler {
    seen: mpsc::UnboundedSender<Bytes>,
}

impl BatchHandler<Vec<PlainMessage>, (), ()> for ReportingBatchHandler {
    type Scratch = ();
    type Taken = Vec<PlainMessage>;

    fn take(batch: Vec<PlainMessage>) -> Vec<PlainMessage> {
        batch
    }

    fn handle_batch<Host: SendHost<()>>(
        &self,
        batch: Vec<PlainMessage>,
        _scratch: &mut Self::Scratch,
        _ctx: &mut Context<'_, (), ()>,
        _host: &Host,
    ) -> impl Future<Output = ()> + Send {
        let seen = self.seen.clone();
        async move {
            for msg in batch {
                seen.send(msg.payload.clone())
                    .expect("the test holds the receiver");
                msg.ack().await.expect("the test delivery settles");
            }
        }
    }
}

/// The worker policies a subscription runs under, each with a dispatch loop of its own:
/// sequential, a pool, and keyed lanes.
const WORKER_FORMS: [Workers; 3] = [
    Workers::sequential(),
    Workers::pool(crate::nonzero!(2usize)),
    Workers::keyed(crate::nonzero!(2usize)),
];

/// The batch loop drives both of its own forms from one place, so both are worth the same
/// shutdown check; keyed lanes do not apply at batch granularity.
const BATCH_FORMS: [Workers; 2] = [
    Workers::sequential(),
    Workers::pool(crate::nonzero!(2usize)),
];

/// Starts the loop `workers` names over `subscriber`, reporting each delivery it handled.
fn reporting_workers<S>(
    subscriber: S,
    workers: Workers,
) -> impl FnOnce(Shutdown, mpsc::UnboundedSender<Bytes>) -> JoinHandle<()>
where
    S: Subscriber<Message = PlainMessage> + Send + 'static,
{
    move |shutdown, seen| {
        spawn_dispatch_workers(
            subscriber,
            Arc::new(ReportingHandler { seen }),
            shutdown,
            Arc::from("orders"),
            Arc::new(()),
            Arc::new(Hosted::empty()),
            dispatch_failure(),
            workers,
        )
        .expect("the dispatch starts")
    }
}

/// Starts the batch loop over `subscriber` under `workers`, reporting each delivery it handled.
fn reporting_batches<S>(
    subscriber: S,
    workers: Workers,
) -> impl FnOnce(Shutdown, mpsc::UnboundedSender<Bytes>) -> JoinHandle<()>
where
    S: BatchSubscriber<Message = PlainMessage, Batch = Vec<PlainMessage>> + Send + 'static,
{
    move |shutdown, seen| {
        spawn_batch_dispatch::<_, _, (), _, _>(
            subscriber,
            Arc::new(ReportingBatchHandler { seen }),
            shutdown,
            Arc::from("orders"),
            Arc::new(()),
            Arc::new(Hosted::empty()),
            dispatch_failure(),
            workers,
            crate::nonzero!(1usize),
        )
        .expect("the dispatch starts")
    }
}

/// Runs one dispatch loop, signals shutdown once the first delivery has been handled, and
/// reports the backlog positions the loop handled before it stopped - or `None` where it never
/// stopped. `spawn` starts the loop under test on the token and the channel its handler reports
/// through.
async fn handled_before_shutdown<Spawn>(spawn: Spawn) -> Option<Vec<usize>>
where
    Spawn: FnOnce(Shutdown, mpsc::UnboundedSender<Bytes>) -> JoinHandle<()>,
{
    let shutdown = Shutdown::new();
    let (seen, mut arrived) = mpsc::unbounded_channel();
    let joined = spawn(shutdown.clone(), seen);
    // One delivery in, so the loop is running rather than about to start.
    let first = arrived.recv().await.expect("a first delivery");
    shutdown.cancel();
    timeout(Duration::from_secs(5), joined)
        .await
        .ok()?
        .expect("dispatch task should not panic");
    // The loop and its handler are gone with the task, so the channel ends where the loop did.
    let mut handled = vec![position_of(&first)];
    while let Some(payload) = arrived.recv().await {
        handled.push(position_of(&payload));
    }
    Some(handled)
}

/// The loop stopped on the signal rather than on the end of the backlog, and took every delivery
/// it handled off the subscription exactly once.
fn assert_stopped_early(handled: &[usize], form: Workers) {
    assert!(
        handled.len() < BACKLOG,
        "{form:?} drained the subscription instead of stopping: {} deliveries",
        handled.len(),
    );
    let mut ordered = handled.to_vec();
    ordered.sort_unstable();
    assert_eq!(
        ordered,
        (0..handled.len()).collect::<Vec<_>>(),
        "{form:?} lost a delivery or handled one twice",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_a_loop_whose_subscription_never_runs_dry() {
    // A saturated subscription: every poll has a delivery ready, so the loop never waits for
    // anything. A loop that consulted the token only where the stream had nothing ready would
    // work through the whole backlog before it noticed.
    for form in WORKER_FORMS {
        let handled = handled_before_shutdown(reporting_workers(SaturatedSubscriber, form))
            .await
            .unwrap_or_else(|| panic!("{form:?} kept consuming after the shutdown signal"));
        assert_stopped_early(&handled, form);
    }
    for form in BATCH_FORMS {
        let handled = handled_before_shutdown(reporting_batches(SaturatedSubscriber, form))
            .await
            .unwrap_or_else(|| panic!("batches under {form:?} kept consuming after the signal"));
        assert_stopped_early(&handled, form);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_wakes_a_loop_parked_on_an_empty_subscription() {
    // The other edge: nothing more is delivered, so the loop is parked when the signal arrives
    // and the token's own wait future is what has to wake it.
    for form in WORKER_FORMS {
        let handled = handled_before_shutdown(reporting_workers(QuietSubscriber, form))
            .await
            .unwrap_or_else(|| panic!("{form:?} never woke on the shutdown signal"));
        assert_eq!(handled, [0], "{form:?} lost or repeated the one delivery");
    }
    for form in BATCH_FORMS {
        let handled = handled_before_shutdown(reporting_batches(QuietSubscriber, form))
            .await
            .unwrap_or_else(|| panic!("batches under {form:?} never woke on the signal"));
        assert_eq!(
            handled,
            [0],
            "batches under {form:?} lost or repeated the one delivery",
        );
    }
}

/// Every loop a subscription can run under, the dedicated threads included: each of them reads
/// its stream to the end on its own.
const ALL_WORKER_FORMS: [Workers; 5] = [
    Workers::sequential(),
    Workers::pool(crate::nonzero!(2usize)),
    Workers::keyed(crate::nonzero!(2usize)),
    Workers::threads(crate::nonzero!(2usize)),
    Workers::threads_keyed(crate::nonzero!(2usize)),
];

/// The batch counterpart of [`ALL_WORKER_FORMS`].
const ALL_BATCH_FORMS: [Workers; 3] = [
    Workers::sequential(),
    Workers::pool(crate::nonzero!(2usize)),
    Workers::threads(crate::nonzero!(2usize)),
];

/// A subscriber whose stream has ended before the loop reads it: the consumer the server
/// cancelled.
struct EndedSubscriber;

impl Subscriber for EndedSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::empty()
    }
}

impl BatchSubscriber for EndedSubscriber {
    type Batch = Vec<PlainMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        batched(self.stream(), size)
    }
}

/// A subscriber whose stream ends because shutdown began: one delivery, then the end the
/// shutdown signal brings, which the loop reads before it reads the signal.
struct EndsOnShutdown(CancellationToken);

impl Subscriber for EndsOnShutdown {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let ended = self.0.clone().cancelled_owned();
        stream::once(ready(Ok(numbered(0)))).chain(stream::once(ended).filter_map(|()| ready(None)))
    }
}

impl BatchSubscriber for EndsOnShutdown {
    type Batch = Vec<PlainMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        batched(self.stream(), size)
    }
}

/// Runs the loop `spawn` starts until it ends on its own, under `on_end`, and hands back the
/// signal it ran on.
async fn ended_under<Spawn>(on_end: SubscriptionEnd, spawn: Spawn, form: Workers) -> Shutdown
where
    Spawn: FnOnce(Shutdown, mpsc::UnboundedSender<Bytes>) -> JoinHandle<()>,
{
    let shutdown = Shutdown::with_subscription_end(on_end);
    let (seen, _arrived) = mpsc::unbounded_channel();
    timeout(Duration::from_secs(5), spawn(shutdown.clone(), seen))
        .await
        .unwrap_or_else(|_| panic!("{form:?} kept running on an ended stream"))
        .expect("dispatch task should not panic");
    shutdown
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_leaves_the_service_running_by_default() {
    let check = |shutdown: &Shutdown, form: Workers| {
        assert!(!shutdown.is_cancelled(), "{form:?} stopped the service");
        assert_eq!(shutdown.peek_failure(), None, "{form:?} reported a failure");
        let count = shutdown.health().probe().subscriptions();
        assert_eq!(
            (count.active, count.started),
            (0, 1),
            "{form:?} still counts the ended subscription as consuming",
        );
    };
    for form in ALL_WORKER_FORMS {
        let spawn = reporting_workers(EndedSubscriber, form);
        check(&ended_under(SubscriptionEnd::Log, spawn, form).await, form);
    }
    for form in ALL_BATCH_FORMS {
        let spawn = reporting_batches(EndedSubscriber, form);
        check(&ended_under(SubscriptionEnd::Log, spawn, form).await, form);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_under_fail_fast_tears_the_service_down() {
    let check = |shutdown: &Shutdown, form: Workers| {
        assert!(shutdown.is_cancelled(), "{form:?} left the service running");
        let reason = shutdown.peek_failure().unwrap_or_default();
        assert!(
            reason.starts_with("orders: "),
            "{form:?} must name the subscription, got: {reason:?}",
        );
    };
    for form in ALL_WORKER_FORMS {
        let spawn = reporting_workers(EndedSubscriber, form);
        check(
            &ended_under(SubscriptionEnd::FailFast, spawn, form).await,
            form,
        );
    }
    for form in ALL_BATCH_FORMS {
        let spawn = reporting_batches(EndedSubscriber, form);
        check(
            &ended_under(SubscriptionEnd::FailFast, spawn, form).await,
            form,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_end_during_shutdown_is_never_reported() {
    async fn run<Spawn>(spawn: impl FnOnce(EndsOnShutdown) -> Spawn, form: Workers)
    where
        Spawn: FnOnce(Shutdown, mpsc::UnboundedSender<Bytes>) -> JoinHandle<()>,
    {
        let shutdown = Shutdown::with_subscription_end(SubscriptionEnd::FailFast);
        let (seen, mut arrived) = mpsc::unbounded_channel();
        let joined = spawn(EndsOnShutdown(shutdown.token().clone()))(shutdown.clone(), seen);
        // One delivery in, so the loop is reading the stream when shutdown ends it.
        arrived.recv().await.expect("a first delivery");
        shutdown.cancel();
        timeout(Duration::from_secs(5), joined)
            .await
            .unwrap_or_else(|_| panic!("{form:?} never stopped"))
            .expect("dispatch task should not panic");
        assert_eq!(
            shutdown.peek_failure(),
            None,
            "{form:?} reported the end that shutdown brought",
        );
    }
    for form in ALL_WORKER_FORMS {
        run(|sub| reporting_workers(sub, form), form).await;
    }
    for form in ALL_BATCH_FORMS {
        run(|sub| reporting_batches(sub, form), form).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_counts_as_consuming_while_its_loop_runs() {
    for form in ALL_WORKER_FORMS {
        let shutdown = Shutdown::new();
        let (seen, mut arrived) = mpsc::unbounded_channel();
        let joined = reporting_workers(QuietSubscriber, form)(shutdown.clone(), seen);
        let count = shutdown.health().probe().subscriptions();
        assert_eq!(
            (count.active, count.started),
            (1, 1),
            "{form:?} must count once from its spawn on, whatever its workers",
        );
        arrived.recv().await.expect("a first delivery");
        shutdown.cancel();
        joined.await.expect("dispatch task should not panic");
        let count = shutdown.health().probe().subscriptions();
        assert_eq!(
            (count.active, count.started),
            (0, 1),
            "{form:?} after shutdown"
        );
    }
}

/// A worker that died yields nothing to account for, and its failure is logged with the cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_worker_is_reported_when_joined() {
    let joined = tokio::spawn(async { panic!("worker down") }).await;
    let (events, guard) = log_capture::start();
    assert!(log_worker_exit(joined).is_none());
    {
        drop(guard);
        let failure = log_capture::find(&events, "worker task failed");
        assert!(
            failure
                .get("error")
                .is_some_and(|err| err.contains("worker down")),
            "the diagnostic must carry the panic: {failure:?}",
        );
    }
}

/// A handler that never finishes, holding the sender it reported its start on: the channel
/// closes only once every copy of the handler and its future are gone.
struct StuckHandler {
    started: mpsc::UnboundedSender<()>,
}

impl Handler<PlainMessage, (), ()> for StuckHandler {
    fn handle(
        &self,
        _msg: &PlainMessage,
        _ctx: &mut Context<'_, (), ()>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        let started = self.started.clone();
        async move {
            started.send(()).expect("the test holds the receiver");
            future::pending::<()>().await;
            HandlerOutcome::ack()
        }
    }
}

// Paused time: the waits below resolve as soon as nothing else can run, which is exactly when
// the loop is parked joining its stuck worker.
#[tokio::test(start_paused = true)]
async fn an_aborted_loop_takes_its_running_workers_down() {
    for form in [WORKER_FORMS[1], WORKER_FORMS[2]] {
        let shutdown = Shutdown::new();
        let (started, mut running) = mpsc::unbounded_channel();
        let mut joined = spawn_dispatch_workers(
            QuietSubscriber,
            Arc::new(StuckHandler { started }),
            shutdown.clone(),
            Arc::from("orders"),
            Arc::new(()),
            Arc::new(Hosted::empty()),
            dispatch_failure(),
            form,
        )
        .expect("the dispatch starts");
        running.recv().await.expect("the handler started");
        shutdown.cancel();
        // The loop now waits for its worker, which the stuck handler never lets finish: what the
        // app's shutdown timeout runs into, and answers by aborting the loop.
        assert!(timeout(Duration::from_secs(1), &mut joined).await.is_err());
        joined.abort();
        let _ = joined.await;
        assert!(
            timeout(Duration::from_secs(1), running.recv())
                .await
                .is_ok_and(|message| message.is_none()),
            "{form:?} left a worker running its handler after the loop was aborted",
        );
    }
}

/// Replays a script and then waits for more that never comes, as a live subscription does.
struct HangingSubscriber {
    items: Vec<PlainMessage>,
}

impl Subscriber for HangingSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::iter(mem::take(&mut self.items).into_iter().map(Ok)).chain(stream::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn a_pool_whose_worker_died_stops_rather_than_shrinks() {
    let items = ["boom", "a", "b", "c"]
        .iter()
        .map(|payload| PlainMessage {
            payload: Bytes::from_static(payload.as_bytes()),
            headers: HeaderMap::new(),
            settlement: if *payload == "boom" {
                Settlement::Panics
            } else {
                Settlement::Accepted
            },
        })
        .collect();
    let (seen, _arrived) = mpsc::unbounded_channel();
    let joined = spawn_dispatch_workers(
        HangingSubscriber { items },
        Arc::new(ReportingHandler { seen }),
        Shutdown::new(),
        Arc::from("orders"),
        Arc::new(()),
        Arc::new(Hosted::empty()),
        dispatch_failure(),
        WORKER_FORMS[1],
    )
    .expect("the dispatch starts");
    // The keyed lanes and the sequential loop stop on a worker that is gone; a pool that kept
    // going on the workers left would run at a capacity nobody configured, and say nothing.
    timeout(Duration::from_secs(1), joined)
        .await
        .expect("the pool kept dispatching on one worker of two")
        .expect("dispatch task should not panic");
}

/// What a broker's own mount step read off the chain when it ran.
#[derive(Debug, PartialEq, Eq)]
struct StepRead {
    workers: usize,
    batch: Option<usize>,
    name: String,
}

/// A broker crate's mount step, the way one ships: an extension trait over the definition's
/// builder that reads what the chain has fixed so far.
trait ReadsTheChain: Sized {
    fn recorded(self, reads: &Mutex<Vec<StepRead>>) -> Self;
}

impl<Def, State, DefCodec> ReadsTheChain for SubscriberBuilder<Def, Name, State, DefCodec> {
    fn recorded(self, reads: &Mutex<Vec<StepRead>>) -> Self {
        let read = StepRead {
            workers: self.dispatch().count().get(),
            batch: self.batch_size().map(NonZeroUsize::get),
            name: self
                .subscription_name::<Connected<MemoryBroker>>()
                .to_owned(),
        };
        reads
            .lock()
            .expect("the record is never poisoned")
            .push(read);
        self
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
struct Confirmation {
    id: u64,
}

impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

/// Confirms an order: the registration with a reply position.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> + Send {
        ready(Ok(Confirmation { id: order.id }))
    }
}

/// Audits an order: the registration the chain goes on to.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> + Send {
        ready(Ok(()))
    }
}

#[test]
fn a_broker_step_on_a_definition_reads_the_chain_on_either_side_of_a_reply_position() {
    let reads = Mutex::new(Vec::new());

    let _routes = Router::<MemoryBroker>::new()
        .include(
            subscriber("orders", Confirm)
                .workers(nonzero!(3))
                .recorded(&reads)
                .reply()
                .to("confirmations")
                .build(),
        )
        .out_reply(MemoryPublish)
        .include(
            subscriber("audits", Audit)
                .workers(nonzero!(2))
                .recorded(&reads)
                .build(),
        );

    assert_eq!(
        reads.into_inner().expect("the record is never poisoned"),
        vec![
            StepRead {
                workers: 3,
                batch: None,
                name: "orders".to_owned(),
            },
            StepRead {
                workers: 2,
                batch: None,
                name: "audits".to_owned(),
            },
        ],
    );
}
