use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use super::super::dispatch::Delivery;
use super::super::failure::ErrorShutdown;
use super::super::input::Decoded;
use super::*;
use crate::codec::JsonCodec;
use crate::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryMessage};
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
        let outcomes: Vec<HandlerResult> = batch
            .iter()
            .map(|n| match n {
                1 => HandlerResult::retry(),
                2 => HandlerResult::drop(),
                _ => HandlerResult::Ack,
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
        let outcomes: Vec<Settle> = batch
            .iter()
            .map(|n| {
                if *n == 0 {
                    let signal = Arc::clone(&signal);
                    HandlerResult::ack().and_after(async move { signal.notify_one() })
                } else {
                    HandlerResult::retry().into()
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
        vec![HandlerResult::Ack]
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
        let outcomes: Vec<HandlerResult> = batch
            .iter()
            .map(|n| match n {
                1 => HandlerResult::retry_after(std::time::Duration::from_secs(5)),
                _ => HandlerResult::Ack,
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
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
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
        HandlerResult::retry()
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
        BatchResult::Uniform(outcome) => outcome,
        other => panic!("expected a uniform settlement, got {other:?}"),
    }
}

fn per_element_outcomes(result: BatchResult) -> Vec<HandlerResult> {
    match result {
        BatchResult::PerElement(settles) => settles.iter().map(Settle::outcome).collect(),
        other => panic!("expected per-element settlements, got {other:?}"),
    }
}

/// Every handler return shape maps onto the settlement the dispatcher applies. The `Result`
/// forms are the interesting ones: an error drops the batch (it is not replayed), while the
/// `Ok` payload decides on its own.
#[test]
fn handler_returns_map_onto_settlements() {
    assert_eq!(
        uniform_outcome(BatchResult::Uniform(HandlerResult::retry()).into_batch_result()),
        HandlerResult::retry(),
    );
    assert_eq!(
        uniform_outcome(HandlerResult::retry().into_batch_result()),
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
        uniform_outcome(Ok::<_, &str>(HandlerResult::retry()).into_batch_result()),
        HandlerResult::retry(),
    );
    assert_eq!(
        uniform_outcome(Err::<HandlerResult, &str>("boom").into_batch_result()),
        HandlerResult::drop(),
    );
    assert_eq!(
        per_element_outcomes(vec![Settle::from(HandlerResult::Ack)].into_batch_result()),
        [HandlerResult::Ack],
    );
    assert_eq!(
        per_element_outcomes(vec![HandlerResult::drop()].into_batch_result()),
        [HandlerResult::drop()],
    );
}

/// A definition that fills in nothing but the required items, to pin what the trait's own
/// defaults contribute to a registration.
struct BareBatch;

impl BatchDef for BareBatch {
    type Input = Decoded<u32>;
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
        |_batch: &[u32], _ctx: &mut Context| async { HandlerResult::Ack },
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
        async { HandlerResult::Ack }
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
        BatchResult::Uniform(HandlerResult::Ack),
        "refusing",
        &TaskTracker::new(),
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
        BatchResult::PerElement(vec![Settle::from(HandlerResult::Ack)]),
        "short-batch",
        &TaskTracker::new(),
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
        HandlerResult::Ack
    });
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("diag-batch", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut sub).await;
    handler.handle_batch(batch, &mut ctx).await;

    settle_batch(
        vec![UnsettleableMessage(Arc::new(AtomicUsize::new(0)))],
        BatchResult::Uniform(HandlerResult::Ack),
        "diag-batch",
        &TaskTracker::new(),
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

/// A batch handler over undecoded payloads, for the raw batch adapter below.
struct Frames(Arc<Mutex<Vec<Vec<u8>>>>);

impl RawSliceHandler for Frames {
    fn handle_slice(
        &self,
        batch: &[&[u8]],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> {
        self.0
            .lock()
            .unwrap()
            .extend(batch.iter().map(|frame| frame.to_vec()));
        ready(BatchResult::Uniform(HandlerResult::Ack))
    }
}

#[tokio::test]
async fn a_raw_batch_lends_the_payloads_and_settles_the_deliveries() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("raw-batch");
    publish_payloads(&broker, "raw-batch", &[b"one", b"two"]).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = RawBatch::over(Frames(Arc::clone(&seen)));
    assert!(format!("{handler:?}").contains("RawBatch"));

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
async fn an_empty_raw_batch_reaches_no_handler() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = RawBatch::over(Frames(Arc::clone(&seen)));

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("raw-batch", &headers, &state, (), &delivery);
    handler
        .handle_batch(Vec::<MemoryMessage>::new(), &mut ctx)
        .await;

    assert!(seen.lock().unwrap().is_empty());
}
