use std::future::ready;
use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use futures::{StreamExt, poll, stream};
use tokio::sync::Notify;
use tokio::time::timeout;

use super::*;
use crate::runtime::redelivery::bare_retry_publisher;

/// The per-delivery context of a registration that reads none: what a test delivery's transforms
/// would see.
fn unit_cx<M>(_msg: &M) {}
use crate::memory::MemoryBroker;
use crate::runtime::failure::{ErrorShutdown, FailurePolicies};
use crate::runtime::handler::HandlerOutcome;
use crate::runtime::handler::HandlerResult;
use crate::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, RetryDeclaration};

/// What a test delivery's transport does when asked to settle. The three cases differ in kind,
/// not degree, so they are variants rather than a flag plus an error slot.
#[derive(Clone, Copy)]
enum Settlement {
    /// Settlement lands, as on a broker with acknowledgements.
    Accepted,
    /// The transport has no settlement at all (MQTT `QoS` 0, `ZeroMQ`, Redis pub/sub).
    Unsupported,
    /// The transport does settle, but the broker rejected this one.
    Rejected,
}

impl Settlement {
    fn apply(self) -> Result<(), AckError> {
        match self {
            Self::Accepted => Ok(()),
            Self::Unsupported => Err(AckError::Unsupported),
            Self::Rejected => Err(AckError::Timeout),
        }
    }
}

/// A delivery without native delayed redelivery: `supports_nack_after` stays at the trait
/// default (`false`), and the default `nack_after` would error. It records how it was settled
/// so a test can assert the fallback dropped it rather than calling `nack(true)`.
struct PlainMessage {
    payload: Bytes,
    headers: HeaderMap,
    // 0 = unset, 1 = nack(false) (dropped), 2 = nack(true) (requeued).
    settled: Arc<AtomicU8>,
    settlement: Settlement,
}

impl IncomingMessage for PlainMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(self.settlement.apply())
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.settled
            .store(if requeue { 2 } else { 1 }, Ordering::SeqCst);
        ready(self.settlement.apply())
    }
}

/// A delivery whose settlement always fails, so the dispatcher's ack-failure path runs.
struct UnsettleableMessage;

impl IncomingMessage for UnsettleableMessage {
    fn payload(&self) -> &[u8] {
        b"body"
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: std::sync::LazyLock<HeaderMap> = std::sync::LazyLock::new(HeaderMap::new);
        &EMPTY
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Timeout))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}

/// A publisher that always rejects, standing in for a broker that died between the nack and
/// the deferred republish.
struct RejectingPublisher;

impl Publisher for RejectingPublisher {
    type Error = std::io::Error;
    type Options = ();

    fn publish(
        &self,
        _msg: OutgoingMessage<'_>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Err(std::io::Error::other("connection closed")))
    }
}

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
        stream::iter(std::mem::take(&mut self.items))
    }
}

/// A backlog long enough that a loop reaching the end of it has plainly drained the
/// subscription rather than stopped on the shutdown signal.
const BACKLOG: usize = 10_000;

/// A subscriber with a long backlog, every delivery ready on the first poll: the loop never
/// waits on it, so the shutdown signal is the only thing that can stop it early.
struct SaturatedSubscriber;

impl Subscriber for SaturatedSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::repeat_with(|| {
            Ok(PlainMessage {
                payload: Bytes::from_static(b"body"),
                headers: HeaderMap::new(),
                settled: Arc::new(AtomicU8::new(0)),
                settlement: Settlement::Accepted,
            })
        })
        .take(BACKLOG)
    }
}

/// A subscriber with one delivery and then nothing: the loop is parked on an empty subscription
/// by the time the test signals shutdown.
struct QuietSubscriber;

impl Subscriber for QuietSubscriber {
    type Message = PlainMessage;
    type Error = StreamFault;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        stream::once(ready(Ok(PlainMessage {
            payload: Bytes::from_static(b"body"),
            headers: HeaderMap::new(),
            settled: Arc::new(AtomicU8::new(0)),
            settlement: Settlement::Accepted,
        })))
        .chain(stream::pending())
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
            settled: Arc::new(AtomicU8::new(0)),
            settlement: Settlement::Accepted,
        })
    }));
    ScriptedSubscriber { items }
}

fn dispatch_failure() -> DispatchFailure {
    DispatchFailure::new(
        FailurePolicies::default(),
        ErrorShutdown::new(CancellationToken::new()),
    )
}

/// Drives one scripted subscriber through `workers` and returns the payloads that reached the
/// handler, in arrival order.
async fn dispatched_under(workers: Workers, payloads: &[&'static str]) -> Vec<Bytes> {
    let (seen, mut arrived) = mpsc::unbounded_channel();
    let joined = spawn_dispatch_workers(
        scripted(payloads),
        Arc::new(ReportingHandler { seen }),
        CancellationToken::new(),
        Arc::from("orders"),
        Arc::new(()),
        Arc::new(Delivery::empty()),
        dispatch_failure(),
        workers,
    );

    let mut handled = Vec::with_capacity(payloads.len());
    for _ in payloads {
        handled.push(arrived.recv().await.expect("delivery should be handled"));
    }
    // The script ends, so the loop terminates on its own rather than on shutdown.
    joined.await.expect("dispatch task should not panic");
    handled
}

fn plain(name_headers: &[(&str, &str)], settled: &Arc<AtomicU8>) -> PlainMessage {
    plain_on(name_headers, settled, Settlement::Accepted)
}

fn plain_on(
    name_headers: &[(&str, &str)],
    settled: &Arc<AtomicU8>,
    settlement: Settlement,
) -> PlainMessage {
    let mut headers = HeaderMap::new();
    for (k, v) in name_headers {
        headers.insert((*k).to_owned(), Bytes::copy_from_slice(v.as_bytes()));
    }
    PlainMessage {
        payload: Bytes::from_static(b"body"),
        headers,
        settled: Arc::clone(settled),
        settlement,
    }
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

/// Counts deliveries and wakes the test on the first one. Unlike `ReportingHandler` it keeps
/// nothing per delivery, so a loop that missed the shutdown signal works through the backlog and
/// fails the count rather than filling memory.
struct CountingHandler {
    handled: Arc<AtomicUsize>,
    first: Arc<Notify>,
}

impl Handler<PlainMessage, (), ()> for CountingHandler {
    fn handle(
        &self,
        _msg: &PlainMessage,
        _ctx: &mut Context<'_, (), ()>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        let handled = Arc::clone(&self.handled);
        let first = Arc::clone(&self.first);
        async move {
            if handled.fetch_add(1, Ordering::Relaxed) == 0 {
                first.notify_one();
            }
            HandlerOutcome::ack()
        }
    }
}

/// Drives `subscriber`, signals shutdown once the first delivery has been handled, and reports
/// how many deliveries the loop handled before it stopped - or `None` where it never stopped.
async fn handled_before_shutdown<S>(subscriber: S) -> Option<usize>
where
    S: Subscriber<Message = PlainMessage> + Send + 'static,
{
    let shutdown = CancellationToken::new();
    let first = Arc::new(Notify::new());
    let handled = Arc::new(AtomicUsize::new(0));
    let joined = spawn_dispatch(
        subscriber,
        Arc::new(CountingHandler {
            handled: Arc::clone(&handled),
            first: Arc::clone(&first),
        }),
        shutdown.clone(),
        Arc::from("orders"),
        Arc::new(()),
        Arc::new(Delivery::empty()),
        dispatch_failure(),
    );
    // One delivery in, so the loop is running rather than about to start.
    first.notified().await;
    shutdown.cancel();
    timeout(Duration::from_secs(5), joined)
        .await
        .ok()
        .map(|joined| {
            joined.expect("dispatch task should not panic");
            handled.load(Ordering::Acquire)
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_a_loop_whose_subscription_never_runs_dry() {
    // A saturated subscription: every poll has a delivery ready, so the loop never waits for
    // anything. A loop that consulted the token only where the stream had nothing ready would
    // work through the whole backlog before it noticed.
    let handled = handled_before_shutdown(SaturatedSubscriber)
        .await
        .expect("the loop kept consuming after the shutdown signal");
    assert!(
        handled < BACKLOG,
        "the loop drained the subscription instead of stopping: {handled} deliveries",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_wakes_a_loop_parked_on_an_empty_subscription() {
    // The other edge: nothing more is delivered, so the loop is parked when the signal arrives
    // and the token's own wait future is what has to wake it.
    assert!(
        handled_before_shutdown(QuietSubscriber).await.is_some(),
        "the parked loop never woke on the shutdown signal",
    );
}

#[tokio::test]
async fn a_failed_acknowledgement_is_logged_rather_than_propagated() {
    // Settlement is best-effort: a broker that rejects the ack must not take the loop down.
    settle_outcome(
        UnsettleableMessage,
        HandlerResult::Ack,
        "orders",
        &Delivery::empty(),
        unit_cx,
    )
    .await;
    settle_outcome(
        UnsettleableMessage,
        HandlerResult::drop(),
        "orders",
        &Delivery::empty(),
        unit_cx,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_deferred_republish_is_logged_rather_than_propagated() {
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(RejectingPublisher),
        "orders",
        TaskTracker::new(),
    );
    let settled = Arc::new(AtomicU8::new(0));
    settle_nack_after(
        plain(&[], &settled),
        "orders",
        Duration::from_secs(1),
        &delivery,
        unit_cx,
    )
    .await
    .unwrap();

    // The original is already dropped, so the failed republish loses the message; the point
    // is that the deferred task reports it instead of panicking the runtime.
    assert_eq!(settled.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
}

#[test]
fn the_default_worker_policy_is_sequential() {
    assert_eq!(Workers::default(), Workers::sequential());
    assert!(Workers::default().is_sequential());
    // One worker of either shape is the sequential loop, not a pool of one.
    assert!(Workers::pool(NonZeroUsize::new(1).unwrap()).is_sequential());
    assert!(!Workers::pool(NonZeroUsize::new(2).unwrap()).is_sequential());
}

#[test]
fn the_delivery_debug_form_reports_wiring_without_leaking_the_publisher() {
    let empty = format!("{:?}", Delivery::<()>::empty());
    assert!(empty.contains("retry_destination: None"), "{empty}");
    assert!(empty.contains("pending_continuations: 0"), "{empty}");

    let wired: Delivery = Delivery::deferring_to(
        bare_retry_publisher(RejectingPublisher),
        "orders",
        TaskTracker::new(),
    );
    assert!(format!("{wired:?}").contains("retry_destination: Some(Some(\"orders\"))"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_worker_is_reported_when_joined() {
    let joined = tokio::spawn(async { panic!("worker down") }).await;
    assert!(joined.is_err());
    log_worker_exit(joined);
}

/// The deferred copy goes to the address the subscription's source reported, which is not the
/// subscription's own name wherever the two are separate resources (a Pub/Sub subscription and
/// its topic). Publishing under the subscription name would reach nothing there.
#[tokio::test(start_paused = true)]
async fn fallback_defers_republish_to_the_reported_address_with_incremented_retry_count() {
    let broker = MemoryBroker::new();
    // Subscribe before publishing: the in-memory broker does not buffer earlier messages.
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(broker.publisher()),
        "orders",
        TaskTracker::new(),
    );

    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain(&[], &settled);
    settle_nack_after(
        msg,
        "orders-workers",
        Duration::from_secs(30),
        &delivery,
        unit_cx,
    )
    .await
    .unwrap();

    // The original is dropped (nack(false)), not requeued, so the broker will not redeliver it.
    assert_eq!(settled.load(Ordering::SeqCst), 1);

    // Nothing is republished before the delay elapses.
    let mut stream = std::pin::pin!(sub.stream());
    assert!(poll!(stream.next()).is_pending());

    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;

    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.payload(), b"body");
    assert_eq!(
        redelivered.headers().get_str(RETRY_COUNT_HEADER),
        Some("1"),
        "the first deferred republish must carry retry-count 1",
    );
}

#[tokio::test(start_paused = true)]
async fn fallback_defers_republish_when_the_transport_cannot_settle() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(broker.publisher()),
        "orders",
        TaskTracker::new(),
    );

    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain_on(&[], &settled, Settlement::Unsupported);
    settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery, unit_cx)
        .await
        .expect("an unsettleable transport is not a settle failure");
    // The drop is still attempted; the transport just has nothing to drop it with.
    assert_eq!(settled.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;

    let mut stream = std::pin::pin!(sub.stream());
    let redelivered = stream
        .next()
        .await
        .expect("the deferred copy is the only way the message survives here")
        .unwrap();
    assert_eq!(redelivered.payload(), b"body");
    assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), Some("1"));
}

#[tokio::test(start_paused = true)]
async fn a_rejected_settle_aborts_the_fallback() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(broker.publisher()),
        "orders",
        TaskTracker::new(),
    );

    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain_on(&[], &settled, Settlement::Rejected);
    let failed = settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery, unit_cx)
        .await
        .expect_err("a broker that rejected the settle must be reported");
    assert!(matches!(failed, AckError::Timeout));
    // The abort happens at the settle, so the drop was attempted before it was reported.
    assert_eq!(settled.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;

    // The original is still the broker's to redeliver, so a deferred copy would duplicate it.
    let mut stream = std::pin::pin!(sub.stream());
    assert!(poll!(stream.next()).is_pending());
}

#[tokio::test(start_paused = true)]
async fn fallback_increments_an_existing_retry_count() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(broker.publisher()),
        "orders",
        TaskTracker::new(),
    );

    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain(&[(RETRY_COUNT_HEADER, "4")], &settled);
    settle_nack_after(msg, "orders", Duration::from_secs(1), &delivery, unit_cx)
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let mut stream = std::pin::pin!(sub.stream());
    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), Some("5"));
}

#[tokio::test]
async fn without_a_copy_path_a_delay_falls_back_to_a_requeue() {
    let delivery = Delivery::empty();
    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain(&[], &settled);
    settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery, unit_cx)
        .await
        .unwrap();
    // The broker moves this subscription's deliveries itself and has no delayed redelivery to
    // ride, so the requeue is all that is left - rather than dropping the message silently.
    assert_eq!(settled.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn native_support_defers_to_the_broker_nack_after() {
    // A native delivery: redelivered by its own timer, never through the retry publisher.
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("orders", b"native"), None)
        .await
        .unwrap();

    // A separate broker backs the retry publisher; if the fallback fired, the republish would
    // land here and never on `sub`.
    let other = MemoryBroker::new();
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(other.publisher()),
        "orders",
        TaskTracker::new(),
    );

    let msg = {
        let mut stream = std::pin::pin!(sub.stream());
        stream.next().await.unwrap().unwrap()
    };
    assert!(msg.supports_nack_after());
    settle_nack_after(msg, "orders", Duration::from_secs(5), &delivery, unit_cx)
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    let mut stream = std::pin::pin!(sub.stream());
    let redelivered = stream.next().await.unwrap().unwrap();
    // Native redelivery keeps the original payload and adds no retry-count header.
    assert_eq!(redelivered.payload(), b"native");
    assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), None);
}

/// A subscription whose deliveries the broker moves itself carries no retry path, so a
/// declaration on it is the broker's to apply: an immediate retry stays the broker's requeue.
#[tokio::test]
async fn a_broker_moved_subscription_keeps_its_own_requeue_under_a_cap() {
    let delivery = Delivery::empty()
        .declaring(RetryDeclaration::new().with_max_attempts(crate::nonzero!(3u32)));
    let settled = Arc::new(AtomicU8::new(0));
    settle_outcome(
        plain(&[], &settled),
        HandlerResult::retry(),
        "orders",
        &delivery,
        unit_cx,
    )
    .await;
    assert_eq!(settled.load(Ordering::SeqCst), 2);
}

/// The same subscription at the cap: the declaration reached the descriptor at startup, so the
/// runtime never reads it again here. A rejection would settle the delivery ahead of the move the
/// broker was about to make, and on a queue that deletes a rejected delivery it would lose it.
#[tokio::test]
async fn a_broker_moved_spent_delivery_stays_the_brokers_requeue() {
    let delivery = Delivery::empty().declaring(
        RetryDeclaration::new()
            .with_max_attempts(crate::nonzero!(1u32))
            .with_dead_letter("orders.dead"),
    );
    let settled = Arc::new(AtomicU8::new(0));
    settle_outcome(
        plain(&[], &settled),
        HandlerResult::retry(),
        "orders",
        &delivery,
        unit_cx,
    )
    .await;
    assert_eq!(settled.load(Ordering::SeqCst), 2);
}

/// A registration that declares nothing keeps the immediate retry it always had, whatever the
/// transport: no copy, no count, the broker's own requeue.
#[tokio::test]
async fn an_undeclared_immediate_retry_stays_a_broker_requeue() {
    let broker = MemoryBroker::new();
    let delivery = Delivery::deferring_to(
        bare_retry_publisher(broker.publisher()),
        "orders",
        TaskTracker::new(),
    );
    let settled = Arc::new(AtomicU8::new(0));
    settle_outcome(
        plain(&[], &settled),
        HandlerResult::retry(),
        "orders",
        &delivery,
        unit_cx,
    )
    .await;
    assert_eq!(settled.load(Ordering::SeqCst), 2);
}
