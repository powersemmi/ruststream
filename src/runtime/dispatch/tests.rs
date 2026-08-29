use std::future::ready;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use futures::StreamExt;

use super::*;
use crate::memory::MemoryBroker;
use crate::runtime::PublishExt;
use crate::runtime::failure::{ErrorShutdown, FailurePolicies};
use crate::runtime::handler::IntoSettle;
use crate::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Publisher};

/// A delivery without native delayed redelivery: `supports_nack_after` stays at the trait
/// default (`false`), and the default `nack_after` would error. It records how it was settled
/// so a test can assert the fallback dropped it rather than calling `nack(true)`.
struct PlainMessage {
    payload: Bytes,
    headers: HeaderMap,
    // 0 = unset, 1 = nack(false) (dropped), 2 = nack(true) (requeued).
    settled: Arc<AtomicU8>,
}

impl IncomingMessage for PlainMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.settled
            .store(if requeue { 2 } else { 1 }, Ordering::SeqCst);
        ready(Ok(()))
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

    fn publish(&self, _msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
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

    fn stream(
        &mut self,
    ) -> impl futures::Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::iter(std::mem::take(&mut self.items))
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
    ) -> impl Future<Output = crate::runtime::Settle> + Send {
        let sent = self.seen.send(msg.payload.clone());
        async move {
            sent.expect("the test holds the receiver");
            HandlerResult::Ack.into_settle()
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
    let mut headers = HeaderMap::new();
    for (k, v) in name_headers {
        headers.insert((*k).to_owned(), Bytes::copy_from_slice(v.as_bytes()));
    }
    PlainMessage {
        payload: Bytes::from_static(b"body"),
        headers,
        settled: Arc::clone(settled),
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

#[tokio::test]
async fn a_failed_acknowledgement_is_logged_rather_than_propagated() {
    // Settlement is best-effort: a broker that rejects the ack must not take the loop down.
    settle_outcome(
        UnsettleableMessage,
        HandlerResult::Ack,
        "orders",
        &Delivery::empty(),
    )
    .await;
    settle_outcome(
        UnsettleableMessage,
        HandlerResult::drop(),
        "orders",
        &Delivery::empty(),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_deferred_republish_is_logged_rather_than_propagated() {
    let delivery = Delivery::detached(Some(Arc::new(RejectingPublisher)), TaskTracker::new());
    let settled = Arc::new(AtomicU8::new(0));
    settle_nack_after(
        plain(&[], &settled),
        "orders",
        Duration::from_secs(1),
        &delivery,
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
    let empty = format!("{:?}", Delivery::empty());
    assert!(empty.contains("retry_publisher: false"), "{empty}");
    assert!(empty.contains("pending_continuations: 0"), "{empty}");

    let wired = Delivery::detached(Some(Arc::new(RejectingPublisher)), TaskTracker::new());
    assert!(format!("{wired:?}").contains("retry_publisher: true"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_worker_is_reported_when_joined() {
    let joined = tokio::spawn(async { panic!("worker down") }).await;
    assert!(joined.is_err());
    log_worker_exit(joined);
}

#[tokio::test(start_paused = true)]
async fn fallback_defers_republish_to_source_with_incremented_retry_count() {
    let broker = MemoryBroker::new();
    // Subscribe before publishing: the in-memory broker does not buffer earlier messages.
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::detached(Some(Arc::new(broker.publisher())), TaskTracker::new());

    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain(&[], &settled);
    settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery)
        .await
        .unwrap();

    // The original is dropped (nack(false)), not requeued, so the broker will not redeliver it.
    assert_eq!(settled.load(Ordering::SeqCst), 1);

    // Nothing is republished before the delay elapses.
    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());

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
async fn fallback_increments_an_existing_retry_count() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::detached(Some(Arc::new(broker.publisher())), TaskTracker::new());

    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain(&[(RETRY_COUNT_HEADER, "4")], &settled);
    settle_nack_after(msg, "orders", Duration::from_secs(1), &delivery)
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let mut stream = std::pin::pin!(sub.stream());
    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), Some("5"));
}

#[tokio::test]
async fn without_a_retry_publisher_the_fallback_requeues_immediately() {
    let delivery = Delivery::empty();
    let settled = Arc::new(AtomicU8::new(0));
    let msg = plain(&[], &settled);
    settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery)
        .await
        .unwrap();
    // No retry publisher: degrade to an immediate requeue rather than dropping silently.
    assert_eq!(settled.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn native_support_defers_to_the_broker_nack_after() {
    // A native delivery: redelivered by its own timer, never through the retry publisher.
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let publisher = broker.publisher();
    publisher
        .raw(b"native")
        .to("orders")
        .publish()
        .await
        .unwrap();

    // A separate broker backs the retry publisher; if the fallback fired, the republish would
    // land here and never on `sub`.
    let other = MemoryBroker::new();
    let delivery = Delivery::detached(Some(Arc::new(other.publisher())), TaskTracker::new());

    let msg = {
        let mut stream = std::pin::pin!(sub.stream());
        stream.next().await.unwrap().unwrap()
    };
    assert!(msg.supports_nack_after());
    settle_nack_after(msg, "orders", Duration::from_secs(5), &delivery)
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
