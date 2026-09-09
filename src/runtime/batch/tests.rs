use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::super::dispatch::{Delivery, RETRY_COUNT_HEADER};
use super::super::failure::ErrorShutdown;
use super::super::input::Decoded;
use super::*;
use crate::codec::JsonCodec;
use crate::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryMessage, MemorySubscriber};
use crate::testkit::batch::{publish_numbers, publish_payloads, pull_batch};
#[cfg(feature = "logging")]
use crate::testkit::log_capture;
use crate::{AckError, HeaderMap, Name, Subscriber, SubscriptionSource};

#[tokio::test]
async fn per_element_outcomes_settle_individually() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("selective");
    publish_numbers(&broker, "selective", &[0, 1, 2]).await;

    // 0 acks, 1 retries, 2 drops: only 1 may come back.
    let handler = typed_batch(JsonCodec, |batch: &[u32], _ctx: &mut Context| {
        let outcomes: Vec<HandlerOutcome> = batch
            .iter()
            .map(|n| match n {
                1 => HandlerOutcome::retry(),
                2 => HandlerOutcome::drop(),
                _ => HandlerOutcome::ack(),
            })
            .collect();
        async move { outcomes }
    });

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("selective", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    assert_eq!(batch.len(), 3);
    handler.handle_batch(batch, &mut ctx).await;

    let redelivered = pull_batch(&mut sub).await;
    let payloads: Vec<&[u8]> = redelivered.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [b"1"]);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_element_continuations_run_after_settle() {
    use tokio::sync::Notify;

    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("after-batch");
    publish_numbers(&broker, "after-batch", &[0, 1]).await;

    // Element 0 acks with a continuation; element 1 retries with no continuation.
    let ran = Arc::new(Notify::new());
    let signal = Arc::clone(&ran);
    let handler = typed_batch(JsonCodec, move |batch: &[u32], _ctx: &mut Context| {
        let signal = Arc::clone(&signal);
        let outcomes: Vec<HandlerOutcome> = batch
            .iter()
            .map(|n| {
                if *n == 0 {
                    let signal = Arc::clone(&signal);
                    HandlerOutcome::ack().and_after(async move { signal.notify_one() })
                } else {
                    HandlerOutcome::retry()
                }
            })
            .collect();
        async move { outcomes }
    });

    let tasks = TaskTracker::new();
    let state = ();
    let delivery = Delivery::with_tasks(tasks.clone());
    let headers = HeaderMap::new();
    let mut ctx = Context::new("after-batch", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    handler.handle_batch(batch, &mut ctx).await;

    // The continuation for element 0 runs on the tracked set after settling.
    ran.notified().await;
    tasks.close();
    tasks.wait().await;

    // Element 1 (no continuation) retried and comes back; element 0 is gone.
    let redelivered = pull_batch(&mut sub).await;
    let payloads: Vec<&[u8]> = redelivered.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [b"1"]);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn unmatched_remainder_is_retried() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("short");
    publish_numbers(&broker, "short", &[0, 1, 2]).await;

    // A buggy handler returning one outcome for a batch of three: the unmatched two retry.
    let handler = typed_batch(JsonCodec, |_batch: &[u32], _ctx: &mut Context| async {
        vec![HandlerOutcome::ack()]
    });

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("short", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    assert_eq!(batch.len(), 3);
    handler.handle_batch(batch, &mut ctx).await;

    let redelivered = pull_batch(&mut sub).await;
    let payloads: Vec<&[u8]> = redelivered.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [b"1", b"2"]);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
}

// Paused time (current-thread runtime): the per-element delay auto-advances.
#[tokio::test(start_paused = true)]
async fn per_element_outcomes_carry_delays() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("delayed");
    publish_numbers(&broker, "delayed", &[0, 1]).await;

    // 0 acks; 1 retries no sooner than five seconds from now.
    let handler = typed_batch(JsonCodec, |batch: &[u32], _ctx: &mut Context| {
        let outcomes: Vec<HandlerOutcome> = batch
            .iter()
            .map(|n| match n {
                1 => HandlerOutcome::retry_after(Duration::from_secs(5)),
                _ => HandlerOutcome::ack(),
            })
            .collect();
        async move { outcomes }
    });

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("delayed", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    handler.handle_batch(batch, &mut ctx).await;

    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.payload(), b"1");
    redelivered.ack().await.unwrap();
}

#[tokio::test]
async fn uniform_outcome_settles_the_whole_batch() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("uniform");
    publish_numbers(&broker, "uniform", &[0, 1]).await;

    let handler = typed_batch(JsonCodec, |_batch: &[u32], _ctx: &mut Context| async {
        HandlerOutcome::retry()
    });

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("uniform", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    assert_eq!(batch.len(), 2);
    handler.handle_batch(batch, &mut ctx).await;

    let redelivered = pull_batch(&mut sub).await;
    assert_eq!(redelivered.len(), 2);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
}

fn uniform_outcome(result: BatchResult) -> HandlerResult {
    match result {
        BatchResult::Uniform(outcome) => outcome.outcome(),
        other => panic!("expected a uniform settlement, got {other:?}"),
    }
}

fn per_element_outcomes(result: BatchResult) -> Vec<HandlerResult> {
    match result {
        BatchResult::PerElement(settles) => settles.iter().map(HandlerOutcome::outcome).collect(),
        other => panic!("expected per-element settlements, got {other:?}"),
    }
}

/// Every handler return shape maps onto the settlement the dispatcher applies. The `Result`
/// forms are the interesting ones: an error drops the batch (it is not replayed), while the
/// `Ok` payload decides on its own.
#[test]
fn handler_returns_map_onto_settlements() {
    assert_eq!(
        uniform_outcome(BatchResult::Uniform(HandlerOutcome::retry()).into_batch_result()),
        HandlerResult::retry(),
    );
    assert_eq!(
        uniform_outcome(HandlerOutcome::retry().into_batch_result()),
        HandlerResult::retry(),
    );
    assert_eq!(uniform_outcome(().into_batch_result()), HandlerResult::Ack);
    assert_eq!(
        uniform_outcome(Ok::<(), &str>(()).into_batch_result()),
        HandlerResult::Ack,
    );
    assert_eq!(
        uniform_outcome(Err::<(), &str>("boom").into_batch_result()),
        HandlerResult::drop(),
    );
    assert_eq!(
        uniform_outcome(Ok::<_, &str>(HandlerOutcome::retry()).into_batch_result()),
        HandlerResult::retry(),
    );
    assert_eq!(
        uniform_outcome(Err::<HandlerOutcome, &str>("boom").into_batch_result()),
        HandlerResult::drop(),
    );
    assert_eq!(
        per_element_outcomes(vec![HandlerOutcome::ack()].into_batch_result()),
        [HandlerResult::Ack],
    );
    assert_eq!(
        per_element_outcomes(vec![HandlerOutcome::drop()].into_batch_result()),
        [HandlerResult::drop()],
    );
}

/// A definition that fills in nothing but the required items, to pin what the trait's own
/// defaults contribute to a registration.
struct BareBatch;

impl BatchDef for BareBatch {
    type Input = Decoded<u32>;
    type Context = ();
    type Handler = ();
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("bare")
    }

    fn into_handler(self) -> Self::Handler {}
}

#[test]
fn batch_def_defaults_register_without_documentation() {
    let def = BareBatch;
    // The mount site names the registration after the def's own source.
    let source = def.source();
    let name = SubscriptionSource::<ConnectedMemoryBroker>::name(&source).to_owned();
    let meta = batch_metadata(name, &def);

    assert_eq!(meta.name, "bare");
    assert_eq!(meta.input_type, "u32");
    assert!(meta.description.is_none());
    assert!(meta.payload_schema.is_none());
    assert!(meta.headers_schema.is_none());
    assert!(meta.message_name.is_none());
    assert!(meta.message_description.is_none());
    assert_eq!(def.workers(), Workers::sequential());
    assert_eq!(def.failure_policies(), FailurePolicies::default());
}

#[test]
fn typed_batch_debug_reports_the_decode_policy() {
    let handler = typed_batch::<MemoryMessage, u32, _, _>(
        JsonCodec,
        |_batch: &[u32], _ctx: &mut Context| async { HandlerOutcome::ack() },
    )
    .with_decode(FailurePolicy::Retry);

    let rendered = format!("{handler:?}");
    assert!(rendered.contains("TypedBatch"), "{rendered}");
    assert!(rendered.contains("Retry"), "{rendered}");
}

/// A `fail_fast` decode policy tears the service down through the context's shutdown handle,
/// drops the offending element (it is not requeued into the failure) and still hands the
/// decodable rest to the handler.
#[tokio::test]
async fn fail_fast_decode_tears_down_and_drops_the_element() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("ff-batch");
    publish_payloads(&broker, "ff-batch", &[b"1", b"not json"]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let collected = Arc::clone(&seen);
    let handler = typed_batch(JsonCodec, move |batch: &[u32], _ctx: &mut Context| {
        collected.lock().unwrap().extend_from_slice(batch);
        async { HandlerOutcome::ack() }
    })
    .with_decode(FailurePolicy::FailFast);

    let token = CancellationToken::new();
    let shutdown = ErrorShutdown::new(token.clone());
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx =
        Context::new("ff-batch", &headers, &state, (), &delivery).with_failfast(&shutdown);
    let batch = pull_batch(&mut sub).await;
    assert_eq!(batch.len(), 2);
    handler.handle_batch(batch, &mut ctx).await;

    assert!(token.is_cancelled(), "a fail-fast decode must tear down");
    let failure = shutdown.peek_failure().expect("a failure must be recorded");
    assert!(failure.contains("ff-batch"), "{failure}");
    assert!(failure.contains("batch decode failed"), "{failure}");
    assert_eq!(*seen.lock().unwrap(), [1]);

    // The undecodable element was dropped, not requeued into the same failure.
    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());
}

/// A delivery whose settlement always fails: the memory broker's own ack cannot fail, so the
/// ack-failure path needs a delivery that refuses.
struct UnsettleableMessage(Arc<AtomicUsize>);

impl IncomingMessage for UnsettleableMessage {
    fn payload(&self) -> &[u8] {
        b"0"
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        EMPTY.get_or_init(HeaderMap::new)
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        ready(Err(AckError::Timeout))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        ready(Err(AckError::Timeout))
    }
}

/// One delivery refusing its ack is a logged diagnostic, not a fatal: the rest of the batch is
/// still settled.
#[tokio::test]
async fn a_refused_ack_does_not_abort_the_batch() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let batch = vec![
        UnsettleableMessage(Arc::clone(&attempts)),
        UnsettleableMessage(Arc::clone(&attempts)),
    ];

    settle_batch(
        batch,
        BatchResult::Uniform(HandlerOutcome::ack()),
        "refusing",
        &Delivery::empty(),
    )
    .await;

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

/// The mismatch diagnostic names the subscription and both counts, so the handler bug behind a
/// short outcome vector is identifiable from the logs alone.
#[cfg(feature = "logging")]
#[tokio::test]
async fn outcome_count_mismatch_is_logged_with_both_counts() {
    let (events, guard) = log_capture::start();

    let attempts = Arc::new(AtomicUsize::new(0));
    let batch = vec![
        UnsettleableMessage(Arc::clone(&attempts)),
        UnsettleableMessage(Arc::clone(&attempts)),
        UnsettleableMessage(Arc::clone(&attempts)),
    ];
    settle_batch(
        batch,
        BatchResult::PerElement(vec![HandlerOutcome::ack()]),
        "short-batch",
        &Delivery::empty(),
    )
    .await;
    drop(guard);

    let mismatch = log_capture::find(
        &events,
        "per-element outcome count does not match the batch; \
             retrying the unmatched remainder",
    );
    assert_eq!(
        mismatch.get("subscription").map(String::as_str),
        Some("short-batch")
    );
    assert_eq!(mismatch.get("expected").map(String::as_str), Some("3"));
    assert_eq!(mismatch.get("returned").map(String::as_str), Some("1"));
}

/// The decode and ack-failure diagnostics carry the subscription (plus the element type and the
/// broker error), so a failure is attributable without a second run.
#[cfg(feature = "logging")]
#[tokio::test]
async fn decode_and_ack_failures_are_logged_with_their_subscription() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("diag-batch");
    publish_payloads(&broker, "diag-batch", &[b"not json"]).await;

    let (events, guard) = log_capture::start();
    let handler = typed_batch(JsonCodec, |_batch: &[u32], _ctx: &mut Context| async {
        HandlerOutcome::ack()
    });
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("diag-batch", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    handler.handle_batch(batch, &mut ctx).await;

    settle_batch(
        vec![UnsettleableMessage(Arc::new(AtomicUsize::new(0)))],
        BatchResult::Uniform(HandlerOutcome::ack()),
        "diag-batch",
        &Delivery::empty(),
    )
    .await;
    drop(guard);

    let decode = log_capture::find(&events, "codec decode failed");
    assert_eq!(
        decode.get("subscription").map(String::as_str),
        Some("diag-batch")
    );
    assert_eq!(decode.get("message_type").map(String::as_str), Some("u32"));

    let ack = log_capture::find(&events, "ack / nack failed");
    assert_eq!(
        ack.get("subscription").map(String::as_str),
        Some("diag-batch")
    );
    assert_eq!(
        ack.get("error").map(String::as_str),
        Some(AckError::Timeout.to_string().as_str())
    );
}

/// A self-deserializing element, for the deserialized batch adapter below: a view over the
/// payload that rejects an empty one, so the construction-failure path is exercisable.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = crate::codec::CodecError;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        if payload.is_empty() {
            return Err(crate::codec::CodecError::Decode(Box::from("empty frame")));
        }
        Ok(Frame(payload))
    }
}

impl crate::runtime::Input for Frame<'_> {
    type Axis = crate::runtime::SoloDeserialized<Frame<'static>>;
}

/// A batch handler over self-constructed payload views, for the adapter below.
struct Frames(Arc<Mutex<Vec<Vec<u8>>>>);

impl<'p> SliceHandler<Frame<'p>> for Frames {
    fn handle_slice(
        &self,
        batch: &[Frame<'p>],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> {
        self.0
            .lock()
            .unwrap()
            .extend(batch.iter().map(|frame| frame.0.to_vec()));
        ready(BatchResult::Uniform(HandlerOutcome::ack()))
    }
}

#[tokio::test]
async fn a_deserialized_batch_lends_the_payloads_and_settles_the_deliveries() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("raw-batch");
    publish_payloads(&broker, "raw-batch", &[b"one", b"two"]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = DeserializedBatch::<_, Frame<'static>, _>::over(Frames(Arc::clone(&seen)));
    assert!(format!("{handler:?}").contains("DeserializedBatch"));

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("raw-batch", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    handler.handle_batch(batch, &mut ctx).await;

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [b"one".to_vec(), b"two".to_vec()],
    );
}

#[tokio::test]
async fn an_empty_deserialized_batch_reaches_no_handler() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = DeserializedBatch::<_, Frame<'static>, _>::over(Frames(Arc::clone(&seen)));

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("raw-batch", &headers, &state, (), &delivery);
    handler
        .handle_batch(Vec::<MemoryMessage>::new(), &mut ctx)
        .await;

    assert!(seen.lock().unwrap().is_empty());
}

/// An element whose construction fails is settled by the decode policy and never reaches the
/// batch; the constructed rest does, exactly like a codec decode failure on the typed path.
#[tokio::test]
async fn a_failed_construction_is_settled_and_the_rest_reach_the_batch() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("raw-batch");
    publish_payloads(&broker, "raw-batch", &[b"one", b"", b"two"]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = DeserializedBatch::<_, Frame<'static>, _>::over(Frames(Arc::clone(&seen)))
        .with_decode(FailurePolicy::Drop);

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("raw-batch", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    handler.handle_batch(batch, &mut ctx).await;

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [b"one".to_vec(), b"two".to_vec()],
    );
}

/// A delivery with no native delayed redelivery: `supports_nack_after` stays at the trait default
/// (`false`), which is what nearly every broker ships, so a `retry_after` settlement has to take
/// the runtime's broker-agnostic fallback. The memory broker's own message answers `true` and
/// would settle natively, hiding the fallback these tests are about.
struct PlainMessage {
    payload: Bytes,
    dropped: Arc<AtomicUsize>,
}

impl PlainMessage {
    /// One delivery per payload, all counting their drops into `dropped`.
    fn batch(payloads: &[&'static [u8]], dropped: &Arc<AtomicUsize>) -> Vec<Self> {
        payloads
            .iter()
            .map(|payload| Self {
                payload: Bytes::from_static(payload),
                dropped: Arc::clone(dropped),
            })
            .collect()
    }
}

impl IncomingMessage for PlainMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        EMPTY.get_or_init(HeaderMap::new)
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        if !requeue {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
        ready(Ok(()))
    }
}

/// The delay every fallback test defers by. Its only requirement is being far enough from zero
/// that the paused clock has to move for the deferred copy to land.
const DEFER: Duration = Duration::from_secs(30);

/// Reads the `expected` deferred copies the fallback re-published, as (payload, retry count)
/// pairs sorted by payload, and asserts nothing else was published.
///
/// The clock is paused, so it advances only when the runtime runs dry - which is exactly when the
/// deferred task is parked on its timer. The outer timeout is another such timer, far past the
/// fallback's, so a lost copy fails the test instead of hanging it.
async fn deferred_copies(
    sub: &mut MemorySubscriber,
    expected: usize,
) -> Vec<(Vec<u8>, Option<String>)> {
    let mut stream = std::pin::pin!(sub.stream());
    let mut copies = Vec::with_capacity(expected);
    for _ in 0..expected {
        let msg = tokio::time::timeout(DEFER * 100, stream.next())
            .await
            .expect("the deferred re-publish is the only retry an unsettled delivery gets")
            .expect("the subscriber outlives the deferred publish")
            .expect("the memory broker delivers what it accepted");
        copies.push((
            msg.payload().to_vec(),
            msg.headers().get_str(RETRY_COUNT_HEADER).map(str::to_owned),
        ));
    }
    assert!(futures::poll!(stream.next()).is_pending());
    copies.sort();
    copies
}

/// A uniform `retry_after` over a batch of deliveries without native delayed redelivery: every
/// element is dropped and re-published after the delay, rather than settled with an unsupported
/// `nack_after` and lost. Covers the uniform arm of `settle_batch`.
#[tokio::test(start_paused = true)]
async fn a_uniform_batch_retry_after_defers_a_republish() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::detached(Some(Arc::new(broker.publisher())), TaskTracker::new());

    let handler = typed_batch(JsonCodec, |_batch: &[u32], _ctx: &mut Context| async {
        HandlerOutcome::retry_after(DEFER)
    });
    let dropped = Arc::new(AtomicUsize::new(0));
    let state = ();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    handler
        .handle_batch(PlainMessage::batch(&[b"1", b"2"], &dropped), &mut ctx)
        .await;

    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert_eq!(
        deferred_copies(&mut sub, 2).await,
        [
            (b"1".to_vec(), Some("1".to_owned())),
            (b"2".to_vec(), Some("1".to_owned())),
        ],
    );
}

/// A per-element `retry_after` next to an ack: only the deferred element comes back, and only
/// after the delay. Covers the per-element arm of `settle_batch`.
#[tokio::test(start_paused = true)]
async fn a_per_element_batch_retry_after_defers_only_its_own_element() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::detached(Some(Arc::new(broker.publisher())), TaskTracker::new());

    let handler = typed_batch(JsonCodec, |_batch: &[u32], _ctx: &mut Context| async {
        vec![HandlerOutcome::retry_after(DEFER), HandlerOutcome::ack()]
    });
    let dropped = Arc::new(AtomicUsize::new(0));
    let state = ();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    handler
        .handle_batch(PlainMessage::batch(&[b"1", b"2"], &dropped), &mut ctx)
        .await;

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        deferred_copies(&mut sub, 1).await,
        [(b"1".to_vec(), Some("1".to_owned()))],
    );
}

/// An element the codec could not decode, under a `retry_after` decode policy: the rejection is
/// deferred rather than dropped on the floor. Covers the decode-rejection settle in
/// `decode_batch`.
#[tokio::test(start_paused = true)]
async fn a_deferred_decode_rejection_is_republished() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::detached(Some(Arc::new(broker.publisher())), TaskTracker::new());

    let handler = typed_batch(JsonCodec, |_batch: &[u32], _ctx: &mut Context| async {
        HandlerOutcome::ack()
    })
    .with_decode(FailurePolicy::RetryAfter(DEFER));
    let dropped = Arc::new(AtomicUsize::new(0));
    let state = ();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    handler
        .handle_batch(
            PlainMessage::batch(&[b"not json", b"1"], &dropped),
            &mut ctx,
        )
        .await;

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        deferred_copies(&mut sub, 1).await,
        [(b"not json".to_vec(), Some("1".to_owned()))],
    );
}

/// A batch handler over self-constructed payload views that defers every element it is given,
/// for the split-batch test below.
struct DeferFrames;

impl<'p> SliceHandler<Frame<'p>> for DeferFrames {
    fn handle_slice(
        &self,
        batch: &[Frame<'p>],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> {
        ready(BatchResult::PerElement(
            batch
                .iter()
                .map(|_| HandlerOutcome::retry_after(DEFER))
                .collect(),
        ))
    }
}

/// A batch whose rejected element was deferred past the handler call, with both the rejection and
/// the handler's own outcome asking for `retry_after`: each takes the fallback on its own. Covers
/// both settles in `settle_split_batch` - the rejected index and the accepted remainder.
#[tokio::test(start_paused = true)]
async fn a_split_batch_defers_the_rejected_and_the_accepted_alike() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    let delivery = Delivery::detached(Some(Arc::new(broker.publisher())), TaskTracker::new());

    // The middle element is empty, which `Frame` refuses to construct from.
    let handler = DeserializedBatch::<_, Frame<'static>, _>::over(DeferFrames)
        .with_decode(FailurePolicy::RetryAfter(DEFER));
    let dropped = Arc::new(AtomicUsize::new(0));
    let state = ();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    handler
        .handle_batch(
            PlainMessage::batch(&[b"one", b"", b"two"], &dropped),
            &mut ctx,
        )
        .await;

    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert_eq!(
        deferred_copies(&mut sub, 3).await,
        [
            (Vec::new(), Some("1".to_owned())),
            (b"one".to_vec(), Some("1".to_owned())),
            (b"two".to_vec(), Some("1".to_owned())),
        ],
    );
}
