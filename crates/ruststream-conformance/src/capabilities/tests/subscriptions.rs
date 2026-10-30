//! Subscriptions whose settlement or seek is broken, and a broker whose connect hangs.

use std::{
    future::pending,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use futures::{Stream, StreamExt, stream};
use tokio::time::sleep;

use super::{LOST_AFTER, on_current_thread_runtime, retaining};
use crate::capabilities::{batch_seeking, batches, seeking, seeking_unknown_position};
use ruststream::memory::{
    ConnectedMemoryBroker, LogMode, MemoryBroker, MemoryError, MemoryMessage, MemoryPosition,
    MemorySeeker, MemorySource, MemorySubscriber, Retaining, Retention,
};
use ruststream_core::{
    AckError, AddressedCopies, BatchSubscriber, Broker, HeaderMap, IncomingMessage, Positioned,
    Seekable, Seeker, Subscribe, Subscriber, SubscriptionSource, nonzero,
};

// The in-memory reference passes the opt-in suites; the others run in `tests/conformance_self.rs`.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_batch_seeking() {
    batch_seeking(
        retaining,
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_seeking_unknown_position() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

// Subscriptions whose settlement or seek is broken.

/// A settlement fault of [`FaultyMessage`].
#[derive(Debug, Clone, Copy)]
enum SettleFault {
    /// A nack with requeue settles the message instead, the way a stand-in that acks on receive
    /// behaves.
    NackDrops,
    /// A nack from a current-thread runtime is left to a task on that runtime.
    NackOnCallerRuntime,
    /// Every ack fails with a timeout.
    AckFails,
    /// Every ack waits forever.
    AckHangs,
    /// Every nack fails with a timeout.
    NackFails,
    /// Every nack answers that the transport has none. The honest answer of a transport with no
    /// requeue.
    NackUnsupported,
}

/// How a subscription double sizes its batches.
#[derive(Debug, Clone, Copy)]
enum BatchFault {
    /// One more element than the size asked for.
    Oversized,
    /// One element per batch, whatever is waiting.
    Singletons,
}

/// A seek fault of [`FaultySeeker`].
#[derive(Debug, Clone, Copy)]
enum SeekFault {
    /// A seek before the subscription's stream opened reports success and does nothing, the way
    /// a consumer refuses a seek before its assignment settled.
    IgnoredBeforeStream,
    /// A seek from a current-thread runtime is left to a task on that runtime.
    OnCallerRuntime,
    /// A seek after shutdown reports success.
    SucceedsAfterShutdown,
    /// A seek to an evicted position repositions to the oldest retained message and reports
    /// success.
    EvictedFallsBackToStart,
    /// A seek to an evicted position reports the eviction, and repositions to the oldest
    /// retained message all the same.
    EvictedRefusedButMoved,
    /// Every seek reports success and does nothing.
    Ignored,
    /// A seek before the subscription's stream opened is refused.
    RefusedBeforeStream,
    /// A seek from a current-thread runtime is refused.
    RefusedOnCallerRuntime,
    /// A seek to an evicted position is refused, the stream yields an error, and the
    /// subscription goes on from the oldest retained message.
    EvictedErrorsAndMoves,
    /// A seek to an evicted position is refused, the stream yields an error, and the
    /// subscription goes on where it was. The honest answer of a broker that reports the refusal
    /// on the stream as well.
    EvictedErrorsAndStays,
    /// A seek to an evicted position is refused, the stream yields an error and ends. Honest too.
    EvictedErrorsAndEnds,
    /// A seek to an evicted position is refused, and the subscription delivers nothing more
    /// without ending.
    EvictedStalls,
    /// A seek to an evicted position is refused, and the stream yields an error on every poll from
    /// then on, each one ready at once.
    EvictedErrorsForever,
}

/// What a subscription double's stream does next, set by a refused seek.
const GOES_ON: u8 = 0;
const ERROR_THEN_GOES_ON: u8 = 1;
const ERROR_THEN_ENDS: u8 = 2;
const STALLS: u8 = 3;
const ERRORS_FOREVER: u8 = 4;

/// The error a subscription double's stream yields.
#[derive(Debug, thiserror::Error)]
#[error("the double's subscription reports a refused seek")]
struct StreamError;

struct FaultySource {
    name: String,
    settle: Option<SettleFault>,
    seek: Option<SeekFault>,
    batch: Option<BatchFault>,
}

impl FaultySource {
    fn behaving(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            settle: None,
            seek: None,
            batch: None,
        }
    }

    fn batching(name: &str, fault: BatchFault) -> Self {
        Self {
            batch: Some(fault),
            ..Self::behaving(name)
        }
    }

    fn settling(name: &str, fault: SettleFault) -> Self {
        Self {
            settle: Some(fault),
            ..Self::behaving(name)
        }
    }

    fn seeking(name: &str, fault: SeekFault) -> Self {
        Self {
            seek: Some(fault),
            ..Self::behaving(name)
        }
    }
}

impl<Log: LogMode> SubscriptionSource<ConnectedMemoryBroker<Log>> for FaultySource {
    type Subscriber = FaultySubscriber<Log>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> Result<Self::Subscriber, MemoryError> {
        Ok(FaultySubscriber {
            inner: Subscribe::subscribe(connected, &self.name).await?,
            settle: self.settle,
            seek: self.seek,
            batch: self.batch,
            streamed: Arc::new(AtomicBool::new(false)),
            next: Arc::new(AtomicU8::new(GOES_ON)),
        })
    }
}

struct FaultySubscriber<Log> {
    inner: MemorySubscriber<Log>,
    settle: Option<SettleFault>,
    seek: Option<SeekFault>,
    batch: Option<BatchFault>,
    streamed: Arc<AtomicBool>,
    /// What the stream does before its next delivery: [`GOES_ON`] or what a refused seek set.
    next: Arc<AtomicU8>,
}

impl<Log: LogMode> Subscriber for FaultySubscriber<Log> {
    type Message = FaultyMessage<Log>;
    type Error = StreamError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.streamed.store(true, Ordering::SeqCst);
        let settle = self.settle;
        let deliveries = self.inner.stream().map(move |item| match item {
            Ok(inner) => FaultyMessage { inner, settle },
            Err(never) => match never {},
        });
        // Read before each delivery, so a refused seek between two reads shows on the next one.
        stream::unfold(
            (Box::pin(deliveries), Arc::clone(&self.next), false),
            async |(mut deliveries, next, ended)| {
                if ended {
                    return None;
                }
                match next.swap(GOES_ON, Ordering::SeqCst) {
                    ERROR_THEN_GOES_ON => Some((Err(StreamError), (deliveries, next, false))),
                    ERROR_THEN_ENDS => Some((Err(StreamError), (deliveries, next, true))),
                    STALLS => pending().await,
                    ERRORS_FOREVER => {
                        next.store(ERRORS_FOREVER, Ordering::SeqCst);
                        Some((Err(StreamError), (deliveries, next, false)))
                    }
                    _ => {
                        let msg = deliveries.next().await?;
                        Some((Ok(msg), (deliveries, next, false)))
                    }
                }
            },
        )
    }
}

impl<Log: LogMode> BatchSubscriber for FaultySubscriber<Log> {
    type Batch = Vec<FaultyMessage<Log>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.streamed.store(true, Ordering::SeqCst);
        let settle = self.settle;
        let size = match self.batch {
            Some(BatchFault::Oversized) => size.saturating_add(1),
            Some(BatchFault::Singletons) => NonZeroUsize::MIN,
            None => size,
        };
        self.inner.batches(size).map(move |item| match item {
            Ok(batch) => Ok(batch
                .into_iter()
                .map(|inner| FaultyMessage { inner, settle })
                .collect()),
            Err(never) => match never {},
        })
    }
}

impl Seekable for FaultySubscriber<Retaining> {
    type Seeker = FaultySeeker;

    fn seeker(&self) -> FaultySeeker {
        FaultySeeker {
            inner: self.inner.seeker(),
            fault: self.seek,
            streamed: Arc::clone(&self.streamed),
            next: Arc::clone(&self.next),
        }
    }
}

struct FaultyMessage<Log> {
    inner: MemoryMessage<Log>,
    settle: Option<SettleFault>,
}

/// Consumes the delivery it holds when it is dropped: the stand-in for a settlement lost with the
/// runtime it was left on, now that an unsettled memory delivery goes back to its subscription.
struct LostWithRuntime<Log: LogMode>(Option<MemoryMessage<Log>>);

impl<Log: LogMode> Drop for LostWithRuntime<Log> {
    fn drop(&mut self) {
        if let Some(unsettled) = self.0.take() {
            // The in-memory settlement happens in the call; the future only carries its answer.
            let _consumed = unsettled.nack(false);
        }
    }
}

impl<Log: LogMode> IncomingMessage for FaultyMessage<Log> {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        match self.settle {
            Some(SettleFault::AckFails) => Err(AckError::Timeout),
            Some(SettleFault::AckHangs) => pending().await,
            _ => self.inner.ack().await,
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self.settle {
            Some(SettleFault::NackFails) => Err(AckError::Timeout),
            // Settled underneath, as a transport with no requeue has already let the message go.
            Some(SettleFault::NackUnsupported) => {
                self.inner.ack().await?;
                Err(AckError::Unsupported)
            }
            Some(SettleFault::NackDrops) if requeue => self.inner.ack().await,
            Some(SettleFault::NackOnCallerRuntime) if on_current_thread_runtime() => {
                let guard = LostWithRuntime(Some(self.inner));
                tokio::spawn(async move {
                    let mut guard = guard;
                    sleep(LOST_AFTER).await;
                    if let Some(inner) = guard.0.take() {
                        let _ = inner.nack(requeue).await;
                    }
                });
                Ok(())
            }
            _ => self.inner.nack(requeue).await,
        }
    }
}

impl Positioned for FaultyMessage<Retaining> {
    type Position = MemoryPosition;

    fn position(&self) -> MemoryPosition {
        self.inner.position()
    }
}

#[derive(Clone)]
struct FaultySeeker {
    inner: MemorySeeker,
    fault: Option<SeekFault>,
    streamed: Arc<AtomicBool>,
    next: Arc<AtomicU8>,
}

impl FaultySeeker {
    /// Seeks to `to`; when the position is evicted, repositions to the start first where `moves`
    /// says so, and sets what the stream does next.
    async fn refuse_evicted(
        &self,
        to: MemoryPosition,
        moves: bool,
        next: u8,
    ) -> Result<(), MemoryError> {
        match self.inner.seek(to).await {
            Err(refused @ MemoryError::PositionEvicted { .. }) => {
                if moves {
                    self.inner.seek(MemoryPosition::start()).await?;
                }
                self.next.store(next, Ordering::SeqCst);
                Err(refused)
            }
            other => other,
        }
    }
}

impl Seeker for FaultySeeker {
    type Position = MemoryPosition;
    type Error = MemoryError;

    async fn seek(&self, to: MemoryPosition) -> Result<(), MemoryError> {
        match self.fault {
            Some(SeekFault::IgnoredBeforeStream) if !self.streamed.load(Ordering::SeqCst) => Ok(()),
            Some(SeekFault::OnCallerRuntime) if on_current_thread_runtime() => {
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    sleep(LOST_AFTER).await;
                    let _ = inner.seek(to).await;
                });
                Ok(())
            }
            Some(SeekFault::SucceedsAfterShutdown) => match self.inner.seek(to).await {
                Err(MemoryError::ShutDown) => Ok(()),
                other => other,
            },
            Some(SeekFault::EvictedFallsBackToStart) => match self.inner.seek(to).await {
                Err(MemoryError::PositionEvicted { .. }) => {
                    self.inner.seek(MemoryPosition::start()).await
                }
                other => other,
            },
            Some(SeekFault::EvictedRefusedButMoved) => match self.inner.seek(to).await {
                Err(refused @ MemoryError::PositionEvicted { .. }) => {
                    self.inner.seek(MemoryPosition::start()).await?;
                    Err(refused)
                }
                other => other,
            },
            Some(SeekFault::Ignored) => Ok(()),
            Some(SeekFault::RefusedBeforeStream) if !self.streamed.load(Ordering::SeqCst) => {
                Err(MemoryError::ShutDown)
            }
            Some(SeekFault::RefusedOnCallerRuntime) if on_current_thread_runtime() => {
                Err(MemoryError::ShutDown)
            }
            Some(SeekFault::EvictedErrorsAndMoves) => {
                self.refuse_evicted(to, true, ERROR_THEN_GOES_ON).await
            }
            Some(SeekFault::EvictedErrorsAndStays) => {
                self.refuse_evicted(to, false, ERROR_THEN_GOES_ON).await
            }
            Some(SeekFault::EvictedErrorsAndEnds) => {
                self.refuse_evicted(to, false, ERROR_THEN_ENDS).await
            }
            Some(SeekFault::EvictedStalls) => self.refuse_evicted(to, false, STALLS).await,
            Some(SeekFault::EvictedErrorsForever) => {
                self.refuse_evicted(to, false, ERRORS_FOREVER).await
            }
            _ => self.inner.seek(to).await,
        }
    }
}

/// With no fault the subscription doubles pass their suites, so a failure below is the check's,
/// not the double's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_subscription_doubles_pass_where_they_behave() {
    batches(MemoryBroker::new, FaultySource::behaving, |broker| {
        broker.publisher()
    })
    .await;
    seeking(retaining, FaultySource::behaving, |broker| {
        broker.publisher()
    })
    .await;
    batch_seeking(retaining, FaultySource::behaving, |broker| {
        broker.publisher()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the element nacked with requeue must come back")]
async fn batches_catch_a_nack_that_settles_the_message() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::settling(name, SettleFault::NackDrops),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nacked with requeue from a runtime that has since stopped must still")]
async fn batches_catch_a_settlement_left_on_the_callers_runtime() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::settling(name, SettleFault::NackOnCallerRuntime),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a seek back must make the next batches replay")]
async fn batch_seeking_catches_an_ignored_seek() {
    batch_seeking(
        retaining,
        |name| FaultySource::seeking(name, SeekFault::Ignored),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a seek made right after subscribing must decide where")]
async fn seeking_catches_a_seek_refused_before_the_first_delivery() {
    seeking(
        retaining,
        |name| FaultySource::seeking(name, SeekFault::IgnoredBeforeStream),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a seek made from a runtime that has since stopped must still")]
async fn seeking_catches_a_seek_left_on_the_callers_runtime() {
    seeking(
        retaining,
        |name| FaultySource::seeking(name, SeekFault::OnCallerRuntime),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a seek through a seeker that outlived the shutdown reported success")]
async fn seeking_catches_a_seek_that_succeeds_after_shutdown() {
    seeking(
        retaining,
        |name| FaultySource::seeking(name, SeekFault::SucceedsAfterShutdown),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a seek to a position the log does not hold reported success")]
async fn seeking_unknown_position_catches_a_fallback_to_the_start() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedFallsBackToStart),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a refused seek must not move the subscription")]
async fn seeking_unknown_position_catches_a_refusal_that_moved_anyway() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedRefusedButMoved),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a batch must never carry more than the size it was opened with")]
async fn batches_catch_a_batch_past_its_size() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::batching(name, BatchFault::Oversized),
        |broker| broker.publisher(),
    )
    .await;
}

/// A subscription that hands out one message per batch is within the batch contract, so it passes
/// the suite; the per-element claim is checked only on a batch that carries several messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_never_groups_waiting_messages_passes() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::batching(name, BatchFault::Singletons),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "batches: ack must succeed or be unsupported, got: Timeout")]
async fn batches_catch_an_ack_that_fails() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::settling(name, SettleFault::AckFails),
        |broker| broker.publisher(),
    )
    .await;
}

/// A settlement that never returns fails the suite at the step bound instead of holding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "an ack hung for")]
async fn batches_catch_an_ack_that_hangs() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::settling(name, SettleFault::AckHangs),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "element by element: nack must succeed or be unsupported, got: Timeout")]
async fn batches_catch_a_nack_that_fails() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::settling(name, SettleFault::NackFails),
        |broker| broker.publisher(),
    )
    .await;
}

/// Nothing comes back from a requeue the transport says it does not perform, and nothing has to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_pass_a_transport_with_no_requeue() {
    batches(
        MemoryBroker::new,
        |name| FaultySource::settling(name, SettleFault::NackUnsupported),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a seek right after subscribing, before the first delivery, was refused")]
async fn seeking_catches_a_seek_refused_before_the_stream_opens() {
    seeking(
        retaining,
        |name| FaultySource::seeking(name, SeekFault::RefusedBeforeStream),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "seeking: a seek from another runtime was refused")]
async fn seeking_catches_a_seek_refused_from_another_runtime() {
    seeking(
        retaining,
        |name| FaultySource::seeking(name, SeekFault::RefusedOnCallerRuntime),
        |broker| broker.publisher(),
    )
    .await;
}

/// Before an error on the stream stopped counting as its end, this double passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a refused seek must not move the subscription")]
async fn seeking_unknown_position_reads_past_an_error_to_a_moved_subscription() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedErrorsAndMoves),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeking_unknown_position_passes_an_error_before_the_next_publish() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedErrorsAndStays),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeking_unknown_position_passes_a_subscription_that_ends_with_the_refusal() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedErrorsAndEnds),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

/// A stream that is ready with an error on every poll after the refusal fails the check at its
/// deadline instead of spinning it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the next publish did not arrive within")]
async fn seeking_unknown_position_catches_a_subscription_that_only_errors_after_the_refusal() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedErrorsForever),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the next publish did not arrive and the subscription did not end")]
async fn seeking_unknown_position_catches_a_subscription_that_stalls_after_the_refusal() {
    seeking_unknown_position(
        || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
        |name| FaultySource::seeking(name, SeekFault::EvictedStalls),
        |broker| broker.publisher(),
        |_| MemoryPosition::sequence(0),
    )
    .await;
}

/// A broker whose connect never returns.
struct Unreachable;

impl Broker for Unreachable {
    type Error = MemoryError;
    type Connected = ConnectedMemoryBroker;

    async fn connect(self) -> Result<ConnectedMemoryBroker, MemoryError> {
        pending().await
    }
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "batches: connect hung for")]
async fn a_suite_fails_a_connect_that_hangs() {
    batches(
        || Unreachable,
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
    )
    .await;
}
