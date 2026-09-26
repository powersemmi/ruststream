//! The in-memory broker passes the opt-in suites, and each new check fails against a broker
//! broken in exactly the way the check exists to catch.
//!
//! The broken doubles wrap the in-memory broker and change one behaviour each: a settlement, a
//! seek, a request, a commit. A fault that leaves work to the caller's runtime does so only on a
//! current-thread runtime, the one a handler on a dedicated thread calls from: on the suite's own
//! multi-threaded runtime the double behaves, so the suite reaches the check it is aimed at.

// The factories' bounds are higher-ranked (`Fn(&str) -> _`, `Fn(&C) -> _`), and a bare path to a
// constructor or a method binds one concrete lifetime, so the closures stay.
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use std::{
    convert::Infallible,
    future::{Future, ready},
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{Stream, StreamExt};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    time::{sleep, timeout},
};

use super::{
    batch_seeking, batches, owned_transactions, request_reply, seeking, seeking_unknown_position,
    transactions,
};
use crate::conformance::helpers::unique_subject;
use crate::memory::{
    ConnectedMemoryBroker, LogMode, MemoryBroker, MemoryError, MemoryMessage, MemoryPosition,
    MemoryPublisher, MemoryRequester, MemorySeeker, MemorySource, MemorySubscriber, RequestError,
    Retaining, Retention,
};
use crate::{
    AckError, AddressedCopies, BatchSubscriber, BytesMut, HeaderMap, IncomingMessage,
    OutgoingMessage, OwnedTransactions, Positioned, Publisher, RequestReply, Seekable, Seeker,
    Subscribe, Subscriber, SubscriptionSource, Take, Transaction, TransactionalPublisher, nonzero,
};

/// How long a task left on a stopping runtime waits before it would act: longer than the rest of
/// the call, so the runtime is gone first.
const LOST_AFTER: Duration = Duration::from_millis(50);

fn retaining() -> MemoryBroker<Retaining> {
    MemoryBroker::retaining(Retention::Messages(nonzero!(64)))
}

/// Whether the caller runs on a current-thread runtime, where a handler on a dedicated thread
/// calls from.
fn on_current_thread_runtime() -> bool {
    Handle::current().runtime_flavor() == RuntimeFlavor::CurrentThread
}

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
}

struct FaultySource {
    name: String,
    settle: Option<SettleFault>,
    seek: Option<SeekFault>,
}

impl FaultySource {
    fn behaving(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            settle: None,
            seek: None,
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
            streamed: Arc::new(AtomicBool::new(false)),
        })
    }
}

struct FaultySubscriber<Log> {
    inner: MemorySubscriber<Log>,
    settle: Option<SettleFault>,
    seek: Option<SeekFault>,
    streamed: Arc<AtomicBool>,
}

impl<Log: LogMode> Subscriber for FaultySubscriber<Log> {
    type Message = FaultyMessage<Log>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.streamed.store(true, Ordering::SeqCst);
        let settle = self.settle;
        self.inner
            .stream()
            .map(move |item| item.map(|inner| FaultyMessage { inner, settle }))
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
        self.inner.batches(size).map(move |item| {
            item.map(|batch| {
                batch
                    .into_iter()
                    .map(|inner| FaultyMessage { inner, settle })
                    .collect()
            })
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
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self.settle {
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

// Requesters whose correlation or shutdown is broken.

/// Publishes a request with `reply_to` in its headers, through a plain publisher.
async fn publish_request(
    publisher: &MemoryPublisher,
    msg: OutgoingMessage<'_, BytesMut>,
    reply_to: &str,
) -> Result<(), RequestError> {
    let (name, payload, mut headers) = msg.into_parts();
    headers.insert("reply-to", reply_to.to_owned());
    publisher
        .publish(
            OutgoingMessage::with_payload(name, payload).with_headers(headers),
            None,
        )
        .await
        .map_err(|_| RequestError::ShutDown)
}

/// Waits for the next message on `inbox`, as the reply.
async fn next_reply(
    inbox: &mut MemorySubscriber,
    subject: &str,
    wait: Duration,
) -> Result<MemoryMessage, RequestError> {
    let mut stream = std::pin::pin!(inbox.stream());
    match timeout(wait, stream.next()).await {
        Ok(Some(Ok(reply))) => Ok(reply),
        Ok(_) => Err(RequestError::ShutDown),
        Err(_) => Err(RequestError::Timeout {
            subject: subject.to_owned(),
            timeout: wait,
        }),
    }
}

/// The requester fault a double has.
#[derive(Debug, Clone, Copy)]
enum RequestFault {
    /// Every request shares one long-lived reply subscription and takes whatever arrives on it
    /// next, a late reply included.
    SharedInbox,
    /// Every request opens its own reply subscription, but under one fixed name, so two requests
    /// in flight at once each see both replies.
    FixedInboxName,
    /// A request whose publish fails waits out its timeout instead of returning the error.
    WaitsAfterShutdown,
}

struct FaultyRequester {
    connected: ConnectedMemoryBroker,
    publisher: MemoryPublisher,
    requester: MemoryRequester,
    inbox_name: String,
    shared: tokio::sync::Mutex<Option<MemorySubscriber>>,
    fault: RequestFault,
}

impl FaultyRequester {
    fn new(connected: &ConnectedMemoryBroker, fault: RequestFault) -> Self {
        Self {
            connected: connected.clone(),
            publisher: connected.publisher(),
            requester: connected.requester(),
            inbox_name: unique_subject("faulty.inbox"),
            shared: tokio::sync::Mutex::new(None),
            fault,
        }
    }
}

impl Publisher for FaultyRequester {
    type Payload = Take;
    type Error = RequestError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&()>,
    ) -> Result<(), RequestError> {
        self.requester.publish(msg, options).await
    }
}

impl RequestReply for FaultyRequester {
    type Reply = MemoryMessage;

    // The shared inbox's lock is held across the whole request on purpose: one reply
    // subscription serves every request in turn, which is the fault that double has.
    #[allow(clippy::significant_drop_tightening)]
    async fn request(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        wait: Duration,
    ) -> Result<MemoryMessage, RequestError> {
        let subject = msg.name().to_owned();
        match self.fault {
            RequestFault::SharedInbox => {
                let mut shared = self.shared.lock().await;
                if shared.is_none() {
                    let inbox = Subscribe::subscribe(&self.connected, &self.inbox_name)
                        .await
                        .map_err(|_| RequestError::ShutDown)?;
                    *shared = Some(inbox);
                }
                let inbox = shared.as_mut().expect("the shared inbox was just opened");
                publish_request(&self.publisher, msg, &self.inbox_name).await?;
                next_reply(inbox, &subject, wait).await
            }
            RequestFault::FixedInboxName => {
                let mut inbox = Subscribe::subscribe(&self.connected, &self.inbox_name)
                    .await
                    .map_err(|_| RequestError::ShutDown)?;
                publish_request(&self.publisher, msg, &self.inbox_name).await?;
                next_reply(&mut inbox, &subject, wait).await
            }
            RequestFault::WaitsAfterShutdown => match self.requester.request(msg, wait).await {
                Err(RequestError::ShutDown) => {
                    sleep(wait).await;
                    Err(RequestError::ShutDown)
                }
                other => other,
            },
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a reply that arrived after its request timed out resolved the next")]
async fn request_reply_catches_a_late_reply_handed_to_the_next_request() {
    request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyRequester::new(broker, RequestFault::SharedInbox),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "two requests in flight at once must each resolve with the reply")]
async fn request_reply_catches_replies_crossed_between_concurrent_requests() {
    request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyRequester::new(broker, RequestFault::FixedInboxName),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a request after shutdown: still pending")]
async fn request_reply_catches_a_request_that_waits_after_shutdown() {
    request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyRequester::new(broker, RequestFault::WaitsAfterShutdown),
        |broker| broker.publisher(),
    )
    .await;
}

// Transactional publishers whose commit or shutdown is broken.

/// A transaction fault of [`FaultyTxPublisher`] and [`FaultyTransaction`].
#[derive(Debug, Clone, Copy)]
enum TxFault {
    /// A commit from a current-thread runtime is left to a task on that runtime.
    CommitOnCallerRuntime,
    /// A commit whose publishes fail reports success.
    CommitSucceedsAfterShutdown,
    /// A direct publish that fails reports success.
    PublishSucceedsAfterShutdown,
    /// A transaction opened once the handle has met the shut-down bus commits with success.
    LateTransactionCommits,
}

/// One message a double's transaction buffers until the commit.
struct Buffered {
    name: String,
    payload: BytesMut,
    headers: HeaderMap,
}

impl From<OutgoingMessage<'_, BytesMut>> for Buffered {
    fn from(msg: OutgoingMessage<'_, BytesMut>) -> Self {
        let (name, payload, headers) = msg.into_parts();
        Self {
            name: name.to_owned(),
            payload,
            headers,
        }
    }
}

/// Publishes every buffered message, in order.
async fn publish_buffer(
    publisher: &MemoryPublisher,
    buffer: Vec<Buffered>,
) -> Result<(), MemoryError> {
    for Buffered {
        name,
        payload,
        headers,
    } in buffer
    {
        publisher
            .publish(
                OutgoingMessage::with_payload(&name, payload).with_headers(headers),
                None,
            )
            .await?;
    }
    Ok(())
}

/// Publishes a committed buffer the way `fault` says.
async fn flush(
    publisher: MemoryPublisher,
    buffer: Vec<Buffered>,
    fault: TxFault,
) -> Result<(), MemoryError> {
    match fault {
        TxFault::CommitOnCallerRuntime if on_current_thread_runtime() => {
            tokio::spawn(async move {
                sleep(LOST_AFTER).await;
                let _ = publish_buffer(&publisher, buffer).await;
            });
            Ok(())
        }
        TxFault::CommitSucceedsAfterShutdown => match publish_buffer(&publisher, buffer).await {
            Err(MemoryError::ShutDown) => Ok(()),
            other => other,
        },
        _ => publish_buffer(&publisher, buffer).await,
    }
}

/// A direct publish the way `fault` says.
async fn publish_directly(
    publisher: &MemoryPublisher,
    msg: OutgoingMessage<'_, BytesMut>,
    fault: TxFault,
) -> Result<(), MemoryError> {
    match (publisher.publish(msg, None).await, fault) {
        (Err(MemoryError::ShutDown), TxFault::PublishSucceedsAfterShutdown) => Ok(()),
        (outcome, _) => outcome,
    }
}

struct FaultyTxPublisher {
    inner: MemoryPublisher,
    open: Mutex<Option<Vec<Buffered>>>,
    fault: TxFault,
}

impl FaultyTxPublisher {
    fn new(connected: &ConnectedMemoryBroker, fault: TxFault) -> Self {
        Self {
            inner: connected.publisher(),
            open: Mutex::new(None),
            fault,
        }
    }

    fn open(&self) -> MutexGuard<'_, Option<Vec<Buffered>>> {
        self.open
            .lock()
            .expect("the double's lock is never poisoned")
    }
}

impl Publisher for FaultyTxPublisher {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> Result<(), MemoryError> {
        let msg = {
            let mut open = self.open();
            match open.as_mut() {
                Some(buffer) => {
                    buffer.push(Buffered::from(msg));
                    return Ok(());
                }
                None => msg,
            }
        };
        publish_directly(&self.inner, msg, self.fault).await
    }
}

impl TransactionalPublisher for FaultyTxPublisher {
    fn begin_transaction(&self) -> impl Future<Output = Result<(), MemoryError>> {
        let mut open = self.open();
        let begun = if open.is_some() {
            Err(MemoryError::TransactionBusy)
        } else {
            *open = Some(Vec::new());
            Ok(())
        };
        drop(open);
        ready(begun)
    }

    async fn commit(&self) -> Result<(), MemoryError> {
        let buffer = self.open().take().ok_or(MemoryError::NoTransaction)?;
        flush(self.inner.clone(), buffer, self.fault).await
    }

    fn abort(&self) -> impl Future<Output = Result<(), MemoryError>> {
        ready(
            self.open()
                .take()
                .map(|_| ())
                .ok_or(MemoryError::NoTransaction),
        )
    }
}

struct FaultyOwnedPublisher {
    inner: MemoryPublisher,
    fault: TxFault,
    /// Set once a publish through this handle met the shut-down bus.
    seen_shutdown: Arc<AtomicBool>,
}

impl FaultyOwnedPublisher {
    fn new(connected: &ConnectedMemoryBroker, fault: TxFault) -> Self {
        Self {
            inner: connected.publisher(),
            fault,
            seen_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Publisher for FaultyOwnedPublisher {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> Result<(), MemoryError> {
        let outcome = publish_directly(&self.inner, msg, self.fault).await;
        if matches!(outcome, Err(MemoryError::ShutDown)) {
            self.seen_shutdown.store(true, Ordering::SeqCst);
        }
        outcome
    }
}

impl OwnedTransactions for FaultyOwnedPublisher {
    type Transaction = FaultyTransaction;

    fn transaction(&self) -> impl Future<Output = Result<FaultyTransaction, MemoryError>> {
        ready(Ok(FaultyTransaction {
            inner: self.inner.clone(),
            buffer: Vec::new(),
            fault: self.fault,
            opened_after_shutdown: self.seen_shutdown.load(Ordering::SeqCst),
        }))
    }
}

struct FaultyTransaction {
    inner: MemoryPublisher,
    buffer: Vec<Buffered>,
    fault: TxFault,
    opened_after_shutdown: bool,
}

impl Transaction for FaultyTransaction {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    fn publish(
        &mut self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), MemoryError>> {
        self.buffer.push(Buffered::from(msg));
        ready(Ok(()))
    }

    async fn commit(self) -> Result<(), MemoryError> {
        if matches!(self.fault, TxFault::LateTransactionCommits) && self.opened_after_shutdown {
            return Ok(());
        }
        flush(self.inner, self.buffer, self.fault).await
    }

    fn abort(self) -> impl Future<Output = Result<(), MemoryError>> {
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "transactions: after a commit from a runtime that has since stopped")]
async fn transactions_catch_a_commit_left_on_the_callers_runtime() {
    transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyTxPublisher::new(broker, TxFault::CommitOnCallerRuntime),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "transactions: a commit after shutdown reported success")]
async fn transactions_catch_a_commit_that_succeeds_after_shutdown() {
    transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyTxPublisher::new(broker, TxFault::CommitSucceedsAfterShutdown),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "transactions: a publish after shutdown reported success")]
async fn transactions_catch_a_publish_that_succeeds_after_shutdown() {
    transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyTxPublisher::new(broker, TxFault::PublishSucceedsAfterShutdown),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "owned_transactions: after a commit from a runtime that has since")]
async fn owned_transactions_catch_a_commit_left_on_the_callers_runtime() {
    owned_transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyOwnedPublisher::new(broker, TxFault::CommitOnCallerRuntime),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the commit of a transaction opened before the shutdown reported")]
async fn owned_transactions_catch_a_commit_that_succeeds_after_shutdown() {
    owned_transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyOwnedPublisher::new(broker, TxFault::CommitSucceedsAfterShutdown),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a direct publish after shutdown reported success")]
async fn owned_transactions_catch_a_publish_that_succeeds_after_shutdown() {
    owned_transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyOwnedPublisher::new(broker, TxFault::PublishSucceedsAfterShutdown),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a transaction opened after the shutdown committed with success")]
async fn owned_transactions_catch_a_late_transaction_that_commits() {
    owned_transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyOwnedPublisher::new(broker, TxFault::LateTransactionCommits),
    )
    .await;
}
