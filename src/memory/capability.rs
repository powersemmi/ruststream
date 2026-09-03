//! Native capability-trait implementations for the in-memory broker.
//!
//! These run on the broker's own in-process semantics: buffered transactions, greedy batching,
//! correlated request / reply over an in-process inbox. The conformance harness offers a suite
//! per capability (see
//! `conformance::capabilities`), and these implementations are the executable reference for
//! them.

use std::{
    fmt,
    future::{Future, ready},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, ready},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, task::AtomicWaker};
use thiserror::Error;
use tokio::{sync::mpsc, time::timeout};
use tracing::warn;

use super::{
    Bus, MemoryDelivery, MemoryError, MemoryMessage, MemoryOutbound, MemoryPublisher, MemoryState,
    MemorySubscriber,
};
use crate::{
    BatchSubscriber, IncomingMessage, OutgoingMessage, OwnedTransactions, Partitioned, Positioned,
    Publisher, RequestReply, Seekable, Seeker, Subscriber, Transaction, TransactionalPublisher,
};

/// The well-known header the [`Partitioned`] implementation reads the partition key from.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// Errors returned by [`MemoryRequester`] operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RequestError {
    /// No reply arrived in the request inbox before the timeout elapsed.
    #[error("no reply to \"{subject}\" within {timeout:?}")]
    Timeout {
        /// The subject the request was published to.
        subject: String,
        /// How long the requester waited for a reply.
        timeout: Duration,
    },
    /// The operation ran through a handle aliasing a bus that was shut down
    /// ([`ConnectedBroker::shutdown`](crate::ConnectedBroker::shutdown)) and not revived by a
    /// sibling clone's [`Broker::connect`](crate::Broker::connect).
    #[error("the memory broker is shut down")]
    ShutDown,
}

/// Publisher with native request / reply support, returned by
/// [`MemoryBroker::requester`](super::MemoryBroker::requester).
///
/// [`request`](RequestReply::request) publishes the message with a unique in-process inbox in
/// the `reply-to` header and resolves on the first message delivered to that inbox. A responder
/// reads `reply-to` from the request headers and publishes its reply there. Plain
/// [`publish`](Publisher::publish) is the usual fire-and-forget fanout.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "macros")]
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// use futures::StreamExt;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::PublishExt;
/// use ruststream::{
///     IncomingMessage, Outgoing, OutgoingMessage, RequestReply, Serialized, Subscriber,
/// };
///
/// // The reply echoes the request's bytes, so they are already the payload; the inbox to
/// // send them to is only known per request, which is what `to(..)` names.
/// #[derive(Outgoing, Serialized)]
/// struct Pong(Vec<u8>);
///
/// let broker = MemoryBroker::new();
/// let mut service = broker.subscribe("svc.echo");
/// let publisher = broker.publisher();
/// let requester = broker.requester();
///
/// let respond = async {
///     let mut stream = std::pin::pin!(service.stream());
///     if let Some(Ok(msg)) = stream.next().await {
///         let reply_to = msg.headers().reply_to().ok_or("request must carry reply-to")?.to_owned();
///         let pong = Pong(msg.payload().to_vec());
///         publisher.message(&pong).to(reply_to).publish().await?;
///         msg.ack().await?;
///     }
///     Ok::<_, Box<dyn std::error::Error>>(())
/// };
/// let request = requester.request(
///     OutgoingMessage::new("svc.echo", b"ping"),
///     Duration::from_secs(1),
/// );
///
/// let (reply, responded) = futures::join!(request, respond);
/// responded?;
/// assert_eq!(reply?.payload(), b"ping");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MemoryRequester {
    state: Arc<MemoryState>,
}

impl MemoryRequester {
    pub(super) fn new(state: Arc<MemoryState>) -> Self {
        Self { state }
    }
}

impl fmt::Debug for MemoryRequester {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryRequester").finish_non_exhaustive()
    }
}

impl Publisher for MemoryRequester {
    type Error = RequestError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        let outbound = MemoryOutbound {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        };
        ready(
            self.state
                .fanout(outbound)
                .map_err(|_| RequestError::ShutDown),
        )
    }
}

impl RequestReply for MemoryRequester {
    type Reply = MemoryMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        wait: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let id = self.state.inbox_seq.fetch_add(1, Ordering::Relaxed);
        let inbox = format!("_inbox.{id}");
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.state
            .register(inbox.clone(), tx.clone())
            .map_err(|_| RequestError::ShutDown)?;

        let mut headers = msg.headers().clone();
        headers.insert("reply-to", inbox.clone());
        let outbound = MemoryOutbound {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers,
        };
        if self.state.fanout(outbound).is_err() {
            self.state.unregister(&inbox);
            return Err(RequestError::ShutDown);
        }

        let outcome = timeout(wait, rx.recv()).await;
        self.state.unregister(&inbox);
        match outcome {
            Ok(Some(reply)) => Ok(MemoryMessage {
                delivery: Some(reply),
                requeue: tx,
                seek: None,
                // The inbox reply is consumed here, not by a dispatch loop, and its enqueue was not
                // counted (see `fanout`), so it carries no coordinator.
                #[cfg(feature = "testing")]
                coordinator: None,
            }),
            // `tx` is held on this stack frame, so the channel cannot report closed.
            Ok(None) => unreachable!("request inbox closed while its sender is held"),
            Err(_) => Err(RequestError::Timeout {
                subject: msg.name().to_owned(),
                timeout: wait,
            }),
        }
    }
}

/// Greedy batching: a batch is the first awaited delivery plus everything already buffered, up
/// to the [`set_batch_limit`](MemorySubscriber::set_batch_limit) cap. Partial batches ship
/// immediately, so no deadline timer is needed.
impl BatchSubscriber for MemorySubscriber {
    type Batch = Vec<MemoryMessage>;

    fn batches(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let limit = self.batch_limit.max(1);
        let requeue = self.requeue.clone();
        #[cfg(feature = "testing")]
        let coordinator = self.coordinator.clone();
        let seeker = Arc::new(Seekable::seeker(self));
        // The drain happens inside a single poll, so no batch state is buffered between polls
        // and the stream stays cancel-safe, like `MemorySubscriber::stream`.
        futures::stream::poll_fn(move |cx| {
            // Same ordering as `MemorySubscriber::stream`: register, then apply a pending seek.
            self.seek.waker.register(cx.waker());
            self.apply_pending_seek();
            let first = loop {
                match ready!(self.rx.poll_recv(cx)) {
                    // A stale pre-seek copy (a requeue that raced the seek): drop it, the
                    // replay already covers everything from the watermark on.
                    Some(delivery) if delivery.seq < self.seek.watermark() => {
                        #[cfg(feature = "testing")]
                        if let Some(coordinator) = &coordinator {
                            coordinator.consumed();
                        }
                    }
                    Some(delivery) => break delivery,
                    None => return Poll::Ready(None),
                }
            };
            let mut batch = vec![MemoryMessage {
                delivery: Some(first),
                requeue: requeue.clone(),
                seek: Some(Arc::clone(&seeker)),
                #[cfg(feature = "testing")]
                coordinator: coordinator.clone(),
            }];
            while batch.len() < limit {
                match self.rx.poll_recv(cx) {
                    // The same stale-copy filter as above, off the batch.
                    Poll::Ready(Some(delivery)) if delivery.seq < self.seek.watermark() =>
                    {
                        #[cfg(feature = "testing")]
                        if let Some(coordinator) = &coordinator {
                            coordinator.consumed();
                        }
                    }
                    Poll::Ready(Some(delivery)) => batch.push(MemoryMessage {
                        delivery: Some(delivery),
                        requeue: requeue.clone(),
                        seek: Some(Arc::clone(&seeker)),
                        #[cfg(feature = "testing")]
                        coordinator: coordinator.clone(),
                    }),
                    // Drained (pending) or closed: ship what we have; a closed channel ends
                    // the stream on the next poll.
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

/// In-process transactions: [`begin_transaction`](TransactionalPublisher::begin_transaction)
/// switches this handle to buffering, [`commit`](TransactionalPublisher::commit) fans out the
/// buffer in publish order, [`abort`](TransactionalPublisher::abort) discards it.
///
/// Misuse errors per the trait contract: a second `begin_transaction` returns
/// [`MemoryError::TransactionBusy`] (leaving the open transaction untouched), and `commit` /
/// `abort` without an open transaction return [`MemoryError::NoTransaction`]. Clones of the
/// handle do not share the transaction (see [`MemoryPublisher`]).
impl TransactionalPublisher for MemoryPublisher {
    fn begin_transaction(&self) -> impl Future<Output = Result<(), MemoryError>> {
        let mut txn = self.txn.lock().expect("memory broker mutex poisoned");
        if txn.is_some() {
            return ready(Err(MemoryError::TransactionBusy));
        }
        *txn = Some(Vec::new());
        drop(txn);
        ready(Ok(()))
    }

    fn commit(&self) -> impl Future<Output = Result<(), MemoryError>> {
        let buffered = self
            .txn
            .lock()
            .expect("memory broker mutex poisoned")
            .take();
        let Some(buffered) = buffered else {
            return ready(Err(MemoryError::NoTransaction));
        };
        for outbound in buffered {
            if let Err(err) = self.state.fanout(outbound) {
                return ready(Err(err));
            }
        }
        ready(Ok(()))
    }

    fn abort(&self) -> impl Future<Output = Result<(), MemoryError>> {
        ready(
            self.txn
                .lock()
                .expect("memory broker mutex poisoned")
                .take()
                .map(|_| ())
                .ok_or(MemoryError::NoTransaction),
        )
    }
}

/// An owned in-process transaction, opened by
/// [`transaction`](crate::OwnedTransactions::transaction) on a [`MemoryPublisher`].
///
/// A private delivery buffer, fanned out to the bus in publish order on commit and discarded
/// on abort.
///
/// Unlike the handle-level [`TransactionalPublisher`] buffer, any number of these can be open
/// on one handle at a time, and the handle keeps publishing directly while they are.
///
/// # Examples
///
/// ```
/// use ruststream::memory::MemoryBroker;
/// use ruststream::{OutgoingMessage, OwnedTransactions, Transaction};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let publisher = MemoryBroker::new().publisher();
/// let mut txn = publisher.transaction().await?;
/// txn.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
/// txn.commit().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct MemoryTransaction {
    state: Arc<MemoryState>,
    buffered: Vec<MemoryOutbound>,
    settled: bool,
}

impl fmt::Debug for MemoryTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryTransaction {
    fn drop(&mut self) {
        // Destructors cannot run async work, so a drop can only discard the buffer; the warning
        // marks that as an abort the caller never wrote, mirroring the runtime's
        // TransactionScope drop.
        if !self.settled {
            warn!(
                target: "ruststream::memory",
                buffered = self.buffered.len(),
                "owned transaction dropped without commit or abort; its buffered messages are \
                 discarded"
            );
        }
    }
}

impl Transaction for MemoryTransaction {
    type Error = MemoryError;

    fn publish(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), MemoryError>> {
        // Buffering is local to this value and never touches the bus; a commit against a
        // shut-down bus is what reports the error.
        self.buffered.push(MemoryOutbound {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        });
        ready(Ok(()))
    }

    fn commit(mut self) -> impl Future<Output = Result<(), MemoryError>> {
        // Settled before the flush: a failed commit has still consumed the transaction (the
        // buffer is lost per the Transaction contract), so the drop warning must not fire.
        self.settled = true;
        for outbound in std::mem::take(&mut self.buffered) {
            if let Err(err) = self.state.fanout(outbound) {
                return ready(Err(err));
            }
        }
        ready(Ok(()))
    }

    fn abort(mut self) -> impl Future<Output = Result<(), MemoryError>> {
        self.settled = true;
        ready(Ok(()))
    }
}

/// Owned transactions: every [`transaction`](OwnedTransactions::transaction) call opens an
/// independent buffer-owning `MemoryTransaction`, so any number can be open concurrently on one
/// handle, next to (and unaffected by) the handle-level [`TransactionalPublisher`] transaction.
impl OwnedTransactions for MemoryPublisher {
    type Transaction = MemoryTransaction;

    fn transaction(&self) -> impl Future<Output = Result<MemoryTransaction, MemoryError>> {
        // Opening allocates a buffer and never touches the bus; a shut-down bus surfaces at
        // commit, the visibility point, like the handle-level begin.
        ready(Ok(MemoryTransaction {
            state: Arc::clone(&self.state),
            buffered: Vec::new(),
            settled: false,
        }))
    }
}

/// The partition key is carried in the [`PARTITION_KEY_HEADER`] header; producers that want
/// per-key ordering set it at publish time. Messages without the header return `None` (any
/// partition will do).
impl Partitioned for MemoryMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// Shared between a subscriber's polling side and the seekers minted off it.
///
/// A seek is a handoff, not an in-place mutation: the seeker records the target and wakes the
/// stream, and the subscriber applies it inside its own poll, where `&mut` access to the
/// receiver is available (see [`MemorySubscriber::apply_pending_seek`]).
#[derive(Default)]
pub(super) struct SeekControl {
    /// The requested log position, taken by the subscriber inside its next poll.
    pending: Mutex<Option<usize>>,
    /// Deliveries stamped below this log position are stale pre-seek copies (a requeue racing
    /// the seek) and are dropped by the polling side.
    watermark: AtomicUsize,
    /// Wakes the subscriber's stream task after `pending` is set.
    pub(super) waker: AtomicWaker,
}

impl SeekControl {
    /// The stale-delivery cutoff, read on the polling side for every delivery.
    ///
    /// Acquire pairs with the Release store in [`Seeker::seek`]: a poll that observes a
    /// delivery enqueued after a seek also observes that seek's watermark.
    pub(super) fn watermark(&self) -> usize {
        self.watermark.load(Ordering::Acquire)
    }
}

/// A position in one name's publish log on the in-memory broker.
///
/// Construct it with [`start`](Self::start) / [`sequence`](Self::sequence), or capture the
/// position of a delivered [`MemoryMessage`] with [`Positioned::position`]. The log is per-name
/// and append-only, so a position is a stable zero-based sequence number: seeking to a captured
/// position redelivers exactly that message, and seeking at or past the end of the log skips
/// everything published so far and resumes with the next publish.
///
/// # Examples
///
/// ```
/// use ruststream::memory::MemoryPosition;
///
/// assert_eq!(MemoryPosition::start(), MemoryPosition::sequence(0));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[must_use]
pub struct MemoryPosition(usize);

impl MemoryPosition {
    /// The start of the log: seeking here replays every message published to the name.
    pub const fn start() -> Self {
        Self(0)
    }

    /// The position of the zero-based `n`-th message published to the name.
    pub const fn sequence(n: usize) -> Self {
        Self(n)
    }

    /// The end of the log: seeking here skips everything published so far and resumes with
    /// the next publish (the memory analog of a "latest" position).
    pub const fn end() -> Self {
        Self(usize::MAX)
    }
}

/// A clonable handle repositioning one [`MemorySubscriber`], minted by
/// [`Seekable::seeker`] before the subscription's stream is opened.
///
/// The scope is that one subscription instance: other subscribers of the same name are
/// unaffected. After the bus shuts down, [`seek`](Seeker::seek) reports
/// [`MemoryError::ShutDown`] like any other aliased handle.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "macros")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use futures::StreamExt;
/// use ruststream::memory::{MemoryBroker, MemoryPosition};
/// use ruststream::runtime::PublishExt;
/// use ruststream::{IncomingMessage, Outgoing, Seekable, Seeker, Serialized, Subscriber};
///
/// // An audit entry is opaque bytes, so it declares itself a serialized type and no codec
/// // runs on it.
/// #[derive(Outgoing, Serialized)]
/// struct Entry(Vec<u8>);
///
/// let broker = MemoryBroker::new();
/// let mut subscriber = broker.subscribe("audit");
/// let seeker = subscriber.seeker();
/// let publisher = broker.publisher();
///
/// publisher.message(&Entry(b"one".to_vec())).to("audit").publish().await?;
/// {
///     let mut stream = std::pin::pin!(subscriber.stream());
///     stream.next().await.expect("delivered")?.ack().await?;
/// }
///
/// // Replay the log from the start: "one" is delivered again.
/// seeker.seek(MemoryPosition::start()).await?;
/// let mut stream = std::pin::pin!(subscriber.stream());
/// let replayed = stream.next().await.expect("replayed")?;
/// assert_eq!(replayed.payload(), b"one");
/// replayed.ack().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MemorySeeker {
    state: Arc<MemoryState>,
    // Arc rather than String: the per-delivery context clones the handle, and the clone must
    // stay allocation-free on the dispatch path.
    name: Arc<str>,
    control: Arc<SeekControl>,
}

impl fmt::Debug for MemorySeeker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySeeker")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Seeking is native here: the broker keeps an append-only publish log per name, so a
/// subscription can re-enqueue any suffix of it.
impl Seekable for MemorySubscriber {
    type Seeker = MemorySeeker;

    fn seeker(&self) -> MemorySeeker {
        MemorySeeker {
            state: Arc::clone(&self.state),
            name: Arc::from(self.name.as_str()),
            control: Arc::clone(&self.seek),
        }
    }
}

impl Seeker for MemorySeeker {
    type Position = MemoryPosition;
    type Error = MemoryError;

    /// Records the reposition and wakes the subscription; the stream applies it at the top of
    /// its next poll, so once this resolves the next delivery reflects the new position.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] through a handle aliasing a shut-down bus.
    fn seek(&self, to: MemoryPosition) -> impl Future<Output = Result<(), MemoryError>> {
        // The liveness check and the handoff happen under the bus lock (then the log lock,
        // fanout's order): a seek that returns Ok happened strictly before any shutdown, and
        // the end-clamp is consistent with the log length at that instant.
        let bus = self
            .state
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        if !matches!(&*bus, Bus::Live(_)) {
            return ready(Err(MemoryError::ShutDown));
        }
        let end = self
            .state
            .published
            .lock()
            .expect("memory broker mutex poisoned")
            .get(&*self.name)
            .map_or(0, Vec::len);
        let clamped = to.0.min(end);
        // Watermark first (Release, paired with the Acquire load in the delivery filter), then
        // the pending target: a poll that takes the target must see its watermark.
        self.control.watermark.store(clamped, Ordering::Release);
        *self
            .control
            .pending
            .lock()
            .expect("memory broker mutex poisoned") = Some(clamped);
        drop(bus);
        self.control.waker.wake();
        ready(Ok(()))
    }
}

impl MemorySubscriber {
    /// Applies a reposition requested through a [`MemorySeeker`], if one is pending.
    ///
    /// Runs at the top of the subscriber's own poll (the stream and batch paths both call it),
    /// which is what makes `&mut` access to the receiver possible: everything queued before
    /// the seek is drained, then the log suffix from the (end-clamped) target on is
    /// re-enqueued. Holding both bus locks makes the swap atomic against `fanout`, which
    /// stamps and sends under the same locks in the same order.
    pub(super) fn apply_pending_seek(&mut self) {
        // Take the target in its own statement so the pending guard is released before the
        // bus lock is acquired; `Seeker::seek` nests the locks in the other direction.
        let pending = self
            .seek
            .pending
            .lock()
            .expect("memory broker mutex poisoned")
            .take();
        let Some(target) = pending else { return };

        let bus = self
            .state
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        // A seek that raced shutdown: the subscription is dead, replay nothing.
        if !matches!(&*bus, Bus::Live(_)) {
            return;
        }
        let log = self
            .state
            .published
            .lock()
            .expect("memory broker mutex poisoned");

        let entries = log
            .get(self.name.as_str())
            .map(Vec::as_slice)
            .unwrap_or_default();

        // The replay is counted in flight BEFORE the drain releases the queued deliveries:
        // decrementing first would let the in-flight count touch zero mid-swap, and a
        // concurrent quiescence wait (`TestApp::settle`) could observe that instant and return
        // before the replayed deliveries were processed.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = &self.coordinator {
            for _ in target..entries.len() {
                coordinator.enqueued();
            }
        }

        while self.rx.try_recv().is_ok() {
            // Every drained delivery was counted in flight when it was enqueued.
            #[cfg(feature = "testing")]
            if let Some(coordinator) = &self.coordinator {
                coordinator.consumed();
            }
        }

        for (seq, raw) in entries.iter().enumerate().skip(target) {
            let delivery = MemoryDelivery {
                name: Arc::from(self.name.as_str()),
                payload: raw.payload_bytes(),
                headers: Arc::new(raw.headers().clone()),
                seq,
            };
            // The send cannot fail: this subscriber holds both ends of its own channel.
            let _ = self.requeue.send(delivery);
        }
        drop(log);
        drop(bus);
    }
}

/// The position is the delivery's stable index in its name's publish log, assigned at fanout
/// and preserved across requeues, so a redelivered message reports the same position.
impl Positioned for MemoryMessage {
    type Position = MemoryPosition;

    fn position(&self) -> MemoryPosition {
        MemoryPosition(self.delivery.as_ref().map_or(0, |d| d.seq))
    }
}

/// The in-memory broker's per-delivery context: this delivery's position in its name's publish
/// log, and the subscription's own seeker.
///
/// The runtime builds one per dispatched delivery (see [`BuildContext`](crate::BuildContext)),
/// and handlers read its fields by key - [`Position`] and [`SeekHandle`] - through the
/// [`Ctx`](crate::runtime::Ctx) extractor or `ctx.context(..)`. A body that manages a log
/// cursor names this type as its `C` axis and needs nothing else.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::{MemoryContext, Position, SeekHandle};
/// use ruststream::prelude::*;
/// use ruststream::Seeker;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<Job, (), (), MemoryContext> for Replayer {
///     async fn handle(
///         &self,
///         job: &Job,
///         _outs: &(),
///         ctx: &mut Context<'_, MemoryContext>,
///     ) -> Result<(), HandlerOutcome> {
///         let here = ctx.context(Position);
///         if job.id == u64::MAX && ctx.context(SeekHandle).seek(here).await.is_err() {
///             return Err(HandlerOutcome::retry());
///         }
///         Ok(())
///     }
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct MemoryContext {
    position: MemoryPosition,
    seeker: MemorySeeker,
}

impl crate::BuildContext<MemoryMessage> for MemoryContext {
    /// # Panics
    ///
    /// Panics on a request-reply inbox message, which is not a subscription delivery; no
    /// dispatch loop (and therefore no per-delivery context) ever builds off one.
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            position: Positioned::position(msg),
            // A clone of the subscription's pre-minted handle: three reference-count bumps,
            // nothing allocated per delivery.
            seeker: msg
                .seek
                .as_deref()
                .expect("a delivery context builds only off subscription deliveries")
                .clone(),
        }
    }
}

/// The in-memory broker's subscription-scoped page context: the subscription's own seeker,
/// shared by every delivery of the page.
///
/// The runtime builds one per dispatched page from the page's first delivery (see
/// [`BuildBatchContext`](crate::BuildBatchContext)), and a page body reads it by key -
/// [`SeekHandle`] - through `ctx.context(..)`. Per-delivery data (a [`Position`]) has no place
/// here: a page spans many deliveries, so the position a body reacts to rides the elements
/// themselves (a `&[Message<H, T>]` page reads it off each element's header contract), and
/// keeping this a separate type from [`MemoryContext`] is what rejects a page body asking for
/// per-delivery fields at compile time.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::Seeker;
/// use ruststream::memory::{MemoryBatchContext, MemoryPosition, SeekHandle};
/// use ruststream::prelude::*;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<[Job], (), (), MemoryBatchContext> for Replayer {
///     async fn handle(
///         &self,
///         page: &[Job],
///         _outs: &(),
///         ctx: &mut Context<'_, MemoryBatchContext>,
///     ) -> Result<(), Vec<HandlerOutcome>> {
///         // A page that saw the rewind marker repositions the whole subscription once it is
///         // settled; the next page opens at the target.
///         if page.iter().any(|job| job.id == u64::MAX)
///             && ctx
///                 .context(SeekHandle)
///                 .seek(MemoryPosition::start())
///                 .await
///                 .is_err()
///         {
///             return Err(page.iter().map(|_| HandlerOutcome::retry()).collect());
///         }
///         Ok(())
///     }
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct MemoryBatchContext {
    seeker: MemorySeeker,
}

impl crate::BuildBatchContext<MemoryMessage> for MemoryBatchContext {
    /// # Panics
    ///
    /// Panics on a request-reply inbox message, which is not a subscription delivery; no
    /// dispatch loop (and therefore no page context) ever builds off one.
    fn build(first: &MemoryMessage) -> Self {
        Self {
            // A clone of the subscription's pre-minted handle: reference-count bumps only,
            // nothing allocated per page.
            seeker: first
                .seek
                .as_deref()
                .expect("a page context builds only off subscription deliveries")
                .clone(),
        }
    }
}

impl crate::Field<MemoryBatchContext> for SeekHandle {
    type Value<'a> = &'a MemorySeeker;

    fn get(self, src: &MemoryBatchContext) -> &MemorySeeker {
        &src.seeker
    }
}

/// The key reading this delivery's [`MemoryPosition`] out of [`MemoryContext`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::Position;
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// #[subscriber("jobs")]
/// async fn audit(job: &Job, Ctx(at): Ctx<Position>) -> HandlerOutcome {
///     println!("job {} sits at {at:?}", job.id);
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Position;

impl crate::ContextField for Position {
    type Context = MemoryContext;
    type Value = MemoryPosition;

    fn read(self, src: &MemoryContext) -> MemoryPosition {
        src.position
    }
}

impl crate::Field<MemoryContext> for Position {
    type Value<'a> = MemoryPosition;

    fn get(self, src: &MemoryContext) -> MemoryPosition {
        src.position
    }
}

/// The key reading the subscription's [`MemorySeeker`] out of [`MemoryContext`]: the reposition
/// handle, resolved once when the subscription opens and carried by every delivery's context.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::{MemoryPosition, SeekHandle};
/// use ruststream::prelude::*;
/// use ruststream::{Seeker, subscriber};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, poisoned_until: Option<usize> }
///
/// /// Skips forward past a region the producer marked poisoned.
/// #[subscriber("jobs")]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if let Some(resume_at) = job.poisoned_until {
///         if seeker.seek(MemoryPosition::sequence(resume_at)).await.is_err() {
///             return HandlerOutcome::retry();
///         }
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SeekHandle;

impl crate::ContextField for SeekHandle {
    type Context = MemoryContext;
    type Value = MemorySeeker;

    fn read(self, src: &MemoryContext) -> MemorySeeker {
        src.seeker.clone()
    }
}

impl crate::Field<MemoryContext> for SeekHandle {
    type Value<'a> = &'a MemorySeeker;

    fn get(self, src: &MemoryContext) -> &MemorySeeker {
        &src.seeker
    }
}

#[cfg(test)]
mod tests;
