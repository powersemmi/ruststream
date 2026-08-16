//! Native capability-trait implementations for the in-memory broker.
//!
//! These are first-class features of the broker's own in-process semantics, not simulations of
//! another broker: real buffered transactions, real greedy batching, real correlated request /
//! reply over an in-process inbox. The conformance harness offers a suite per capability (see
//! `conformance::capabilities`), and these implementations are the executable reference for
//! them.

use std::{
    fmt,
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
/// use std::time::Duration;
///
/// use futures::StreamExt;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::{IncomingMessage, OutgoingMessage, Publisher, RequestReply, Subscriber};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = MemoryBroker::new();
/// let mut service = broker.subscribe("svc.echo");
/// let publisher = broker.publisher();
/// let requester = broker.requester();
///
/// let respond = async {
///     let mut stream = std::pin::pin!(service.stream());
///     if let Some(Ok(msg)) = stream.next().await {
///         let reply_to = msg.headers().reply_to().ok_or("request must carry reply-to")?.to_owned();
///         publisher.publish(OutgoingMessage::new(&reply_to, msg.payload())).await?;
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

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let outbound = MemoryOutbound {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        };
        self.state
            .fanout(outbound)
            .map_err(|_| RequestError::ShutDown)
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
    async fn begin_transaction(&self) -> Result<(), MemoryError> {
        let mut txn = self.txn.lock().expect("memory broker mutex poisoned");
        if txn.is_some() {
            return Err(MemoryError::TransactionBusy);
        }
        *txn = Some(Vec::new());
        drop(txn);
        Ok(())
    }

    async fn commit(&self) -> Result<(), MemoryError> {
        let buffered = self
            .txn
            .lock()
            .expect("memory broker mutex poisoned")
            .take()
            .ok_or(MemoryError::NoTransaction)?;
        for outbound in buffered {
            self.state.fanout(outbound)?;
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), MemoryError> {
        self.txn
            .lock()
            .expect("memory broker mutex poisoned")
            .take()
            .map(|_| ())
            .ok_or(MemoryError::NoTransaction)
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

    async fn publish(&mut self, msg: OutgoingMessage<'_>) -> Result<(), MemoryError> {
        // Buffering is local to this value and never touches the bus; a commit against a
        // shut-down bus is what reports the error.
        self.buffered.push(MemoryOutbound {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        });
        Ok(())
    }

    async fn commit(mut self) -> Result<(), MemoryError> {
        // Settled before the flush: a failed commit has still consumed the transaction (the
        // buffer is lost per the Transaction contract), so the drop warning must not fire.
        self.settled = true;
        for outbound in std::mem::take(&mut self.buffered) {
            self.state.fanout(outbound)?;
        }
        Ok(())
    }

    async fn abort(mut self) -> Result<(), MemoryError> {
        self.settled = true;
        Ok(())
    }
}

/// Owned transactions: every [`transaction`](OwnedTransactions::transaction) call opens an
/// independent buffer-owning `MemoryTransaction`, so any number can be open concurrently on one
/// handle, next to (and unaffected by) the handle-level [`TransactionalPublisher`] transaction.
impl OwnedTransactions for MemoryPublisher {
    type Transaction = MemoryTransaction;

    async fn transaction(&self) -> Result<MemoryTransaction, MemoryError> {
        // Opening allocates a buffer and never touches the bus; a shut-down bus surfaces at
        // commit, the visibility point, like the handle-level begin.
        Ok(MemoryTransaction {
            state: Arc::clone(&self.state),
            buffered: Vec::new(),
            settled: false,
        })
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
/// use futures::StreamExt;
/// use ruststream::memory::{MemoryBroker, MemoryPosition};
/// use ruststream::{IncomingMessage, OutgoingMessage, Publisher, Seekable, Seeker, Subscriber};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = MemoryBroker::new();
/// let mut subscriber = broker.subscribe("audit");
/// let seeker = subscriber.seeker();
/// let publisher = broker.publisher();
///
/// publisher.publish(OutgoingMessage::new("audit", b"one".as_slice())).await?;
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
    name: String,
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
            name: self.name.clone(),
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
    async fn seek(&self, to: MemoryPosition) -> Result<(), MemoryError> {
        // The liveness check and the handoff happen under the bus lock (then the log lock,
        // fanout's order): a seek that returns Ok happened strictly before any shutdown, and
        // the end-clamp is consistent with the log length at that instant.
        let bus = self
            .state
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        if !matches!(&*bus, Bus::Live(_)) {
            return Err(MemoryError::ShutDown);
        }
        let end = self
            .state
            .published
            .lock()
            .expect("memory broker mutex poisoned")
            .get(self.name.as_str())
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
        Ok(())
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::super::{MemoryBroker, MemorySource};
    use super::*;
    #[cfg(feature = "testing")]
    use crate::Subscribe;
    #[cfg(feature = "testing")]
    use crate::testing::{TestableBroker, coordinator::Coordinator};
    use crate::{Broker, ConnectedBroker, Headers, StartAt, SubscriptionSource};

    #[tokio::test]
    async fn batches_drain_buffered_deliveries() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("batch");
        let publisher = broker.publisher();
        for i in 0..5u8 {
            publisher
                .publish(OutgoingMessage::new("batch", &[i]))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches());
        let batch = stream.next().await.unwrap().unwrap();
        let payloads: Vec<u8> = batch.iter().map(|m| m.payload()[0]).collect();
        assert_eq!(payloads, [0, 1, 2, 3, 4]);
        for msg in batch {
            msg.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn batch_limit_caps_each_batch() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("batch.capped");
        sub.set_batch_limit(2);
        let publisher = broker.publisher();
        for i in 0..3u8 {
            publisher
                .publish(OutgoingMessage::new("batch.capped", &[i]))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches());
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.len(), 2);
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.len(), 1);
        for msg in first.into_iter().chain(second) {
            msg.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn transaction_buffers_until_commit() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("txn");
        let publisher = broker.publisher();

        publisher.begin_transaction().await.unwrap();
        publisher
            .publish(OutgoingMessage::new("txn", b"a".as_slice()))
            .await
            .unwrap();
        publisher
            .publish(OutgoingMessage::new("txn", b"b".as_slice()))
            .await
            .unwrap();

        // Fanout is synchronous, so an empty queue here proves nothing was published yet.
        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        publisher.commit().await.unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.payload(), b"a");
        first.ack().await.unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.payload(), b"b");
        second.ack().await.unwrap();
    }

    #[tokio::test]
    async fn abort_discards_buffered_publishes() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("txn.abort");
        let publisher = broker.publisher();

        publisher.begin_transaction().await.unwrap();
        publisher
            .publish(OutgoingMessage::new("txn.abort", b"gone".as_slice()))
            .await
            .unwrap();
        publisher.abort().await.unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("txn.abort", b"kept".as_slice()))
            .await
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"kept");
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    async fn clone_does_not_join_transaction() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("txn.clone");
        let transactional = broker.publisher();

        transactional.begin_transaction().await.unwrap();
        transactional
            .publish(OutgoingMessage::new("txn.clone", b"buffered".as_slice()))
            .await
            .unwrap();

        let independent = transactional.clone();
        independent
            .publish(OutgoingMessage::new("txn.clone", b"direct".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.payload(), b"direct");
        first.ack().await.unwrap();

        transactional.commit().await.unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.payload(), b"buffered");
        second.ack().await.unwrap();
    }

    #[tokio::test]
    async fn transactional_misuse_errors() {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();

        assert_eq!(publisher.commit().await, Err(MemoryError::NoTransaction));
        assert_eq!(publisher.abort().await, Err(MemoryError::NoTransaction));

        publisher.begin_transaction().await.unwrap();
        assert_eq!(
            publisher.begin_transaction().await,
            Err(MemoryError::TransactionBusy),
        );
        // The rejected second begin left the transaction open; an abort settles it.
        publisher.abort().await.unwrap();
        assert_eq!(publisher.abort().await, Err(MemoryError::NoTransaction));
    }

    #[tokio::test]
    async fn commit_after_shutdown_errors() {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();

        publisher.begin_transaction().await.unwrap();
        publisher
            .publish(OutgoingMessage::new("txn.down", b"buffered".as_slice()))
            .await
            .unwrap();
        let connected = broker.connect().await.unwrap();
        connected.shutdown().await.unwrap();

        // Buffering never touched the bus, so the shutdown surfaces at the visibility point.
        assert_eq!(publisher.commit().await, Err(MemoryError::ShutDown));
    }

    #[tokio::test]
    async fn requester_errors_after_shutdown() {
        let broker = MemoryBroker::new();
        let requester = broker.requester();
        let connected = broker.connect().await.unwrap();
        connected.shutdown().await.unwrap();

        let publish = Publisher::publish(
            &requester,
            OutgoingMessage::new("svc.echo", b"ping".as_slice()),
        )
        .await;
        assert!(
            matches!(publish, Err(RequestError::ShutDown)),
            "{publish:?}"
        );

        let request = requester
            .request(
                OutgoingMessage::new("svc.echo", b"ping".as_slice()),
                Duration::from_millis(50),
            )
            .await;
        assert!(
            matches!(request, Err(RequestError::ShutDown)),
            "a request against a dead bus must fail fast, not time out",
        );
    }

    #[tokio::test]
    async fn request_resolves_on_reply() {
        let broker = MemoryBroker::new();
        let mut service = broker.subscribe("svc.echo");
        let publisher = broker.publisher();
        let requester = broker.requester();

        let respond = async {
            let mut stream = std::pin::pin!(service.stream());
            let msg = stream.next().await.unwrap().unwrap();
            assert_eq!(msg.payload(), b"ping");
            let reply_to = msg.headers().reply_to().unwrap().to_owned();
            publisher
                .publish(OutgoingMessage::new(&reply_to, b"pong".as_slice()))
                .await
                .unwrap();
            msg.ack().await.unwrap();
        };
        let request = requester.request(
            OutgoingMessage::new("svc.echo", b"ping".as_slice()),
            Duration::from_secs(1),
        );

        let (reply, ()) = futures::join!(request, respond);
        assert_eq!(reply.unwrap().payload(), b"pong");

        // The single-use inbox must be unregistered once the request resolves.
        let inbox_leaked = match &*broker.state.subscribers.lock().unwrap() {
            Bus::Live(subscribers) => subscribers.keys().any(|name| name.starts_with("_inbox.")),
            Bus::ShutDown => false,
        };
        assert!(!inbox_leaked);
    }

    // Paused time needs the current-thread runtime; the test spawns nothing, so the timeout
    // auto-advances instead of sleeping for real.
    #[tokio::test(start_paused = true)]
    async fn request_times_out_without_responder() {
        let broker = MemoryBroker::new();
        let requester = broker.requester();

        let outcome = requester
            .request(
                OutgoingMessage::new("svc.void", b"ping".as_slice()),
                Duration::from_millis(5),
            )
            .await;
        assert!(matches!(outcome, Err(RequestError::Timeout { .. })));

        let inbox_leaked = match &*broker.state.subscribers.lock().unwrap() {
            Bus::Live(subscribers) => subscribers.keys().any(|name| name.starts_with("_inbox.")),
            Bus::ShutDown => false,
        };
        assert!(!inbox_leaked);
    }

    #[tokio::test]
    async fn seek_back_redelivers_from_the_captured_position() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.back");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.back", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        let mut positions = Vec::new();
        for _ in 0..3 {
            let msg = stream.next().await.unwrap().unwrap();
            positions.push(msg.position());
            msg.ack().await.unwrap();
        }

        seeker.seek(positions[1]).await.unwrap();
        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.payload(), b"b");
        // The replayed copy reports the same position as the original delivery.
        assert_eq!(redelivered.position(), positions[1]);
        redelivered.ack().await.unwrap();
        let tail = stream.next().await.unwrap().unwrap();
        assert_eq!(tail.payload(), b"c");
        tail.ack().await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[tokio::test]
    async fn constructed_position_seeks_forward_skipping_queued() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.fwd");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.fwd", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.payload(), b"a");
        first.ack().await.unwrap();

        // "b" is still queued; jumping to the third message must skip it.
        seeker.seek(MemoryPosition::sequence(2)).await.unwrap();
        let skipped_to = stream.next().await.unwrap().unwrap();
        assert_eq!(skipped_to.payload(), b"c");
        skipped_to.ack().await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[tokio::test]
    async fn stale_requeue_racing_a_seek_is_dropped() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.stale");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.stale", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        let held = stream.next().await.unwrap().unwrap();
        assert_eq!(held.payload(), b"a");

        seeker.seek(MemoryPosition::sequence(2)).await.unwrap();
        let skipped_to = stream.next().await.unwrap().unwrap();
        assert_eq!(skipped_to.payload(), b"c");
        skipped_to.ack().await.unwrap();

        // The requeue lands after the seek; its copy is below the watermark and is dropped
        // instead of resurrecting a delivery the reposition already skipped.
        held.nack(true).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("seek.stale", b"d".as_slice()))
            .await
            .unwrap();
        let live = stream.next().await.unwrap().unwrap();
        assert_eq!(live.payload(), b"d");
        live.ack().await.unwrap();
    }

    #[tokio::test]
    async fn seek_past_the_end_skips_the_queue_and_resumes_with_the_next_publish() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.end");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("seek.end", payload.as_slice()))
                .await
                .unwrap();
        }

        // The target is clamped to the log end at seek time, so everything queued is skipped
        // and the next publish is not filtered away.
        seeker.seek(MemoryPosition::sequence(10)).await.unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("seek.end", b"c".as_slice()))
            .await
            .unwrap();
        let live = stream.next().await.unwrap().unwrap();
        assert_eq!(live.payload(), b"c");
        live.ack().await.unwrap();
    }

    #[tokio::test]
    async fn start_at_replays_the_log_into_a_fresh_subscription() {
        let broker = MemoryBroker::new();
        let connected = broker.connect().await.unwrap();
        let publisher = connected.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("start.replay", payload.as_slice()))
                .await
                .unwrap();
        }

        // The subscription opens after both publishes; the start position replays them.
        let mut sub = StartAt::new(MemorySource::new("start.replay"), MemoryPosition::start())
            .subscribe(&connected)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.payload(), b"a");
        first.ack().await.unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.payload(), b"b");
        second.ack().await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[tokio::test]
    async fn start_at_end_skips_history_and_sees_the_next_publish() {
        let broker = MemoryBroker::new();
        let connected = broker.connect().await.unwrap();
        let publisher = connected.publisher();
        publisher
            .publish(OutgoingMessage::new("start.end", b"old".as_slice()))
            .await
            .unwrap();

        let mut sub = StartAt::new(MemorySource::new("start.end"), MemoryPosition::end())
            .subscribe(&connected)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("start.end", b"new".as_slice()))
            .await
            .unwrap();
        let live = stream.next().await.unwrap().unwrap();
        assert_eq!(live.payload(), b"new");
        live.ack().await.unwrap();
    }

    #[tokio::test]
    async fn seeker_errors_after_shutdown() {
        let broker = MemoryBroker::new();
        let sub = broker.subscribe("seek.down");
        let seeker = sub.seeker();
        let connected = broker.connect().await.unwrap();
        connected.shutdown().await.unwrap();

        assert_eq!(
            seeker.seek(MemoryPosition::start()).await,
            Err(MemoryError::ShutDown),
        );
    }

    #[tokio::test]
    async fn batches_replay_after_a_seek() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.batch");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.batch", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches());
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 3);
        for msg in batch {
            msg.ack().await.unwrap();
        }

        seeker.seek(MemoryPosition::start()).await.unwrap();
        let replayed = stream.next().await.unwrap().unwrap();
        let payloads: Vec<&[u8]> = replayed.iter().map(IncomingMessage::payload).collect();
        assert_eq!(
            payloads,
            [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
        for msg in replayed {
            msg.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn position_is_stable_across_a_requeue() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.requeue");
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("seek.requeue", b"a".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        let position = msg.position();
        msg.nack(true).await.unwrap();

        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.position(), position);
        redelivered.ack().await.unwrap();
    }

    // The wake path is the point of the capability: a dispatch loop parked on an empty
    // subscription must observe a seek without waiting for an unrelated publish, so this test
    // parks a real task (the oneshot fires on its first Pending poll) before seeking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seek_wakes_a_parked_stream() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.wake");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("seek.wake", b"a".as_slice()))
            .await
            .unwrap();
        {
            let mut stream = std::pin::pin!(sub.stream());
            stream.next().await.unwrap().unwrap().ack().await.unwrap();
        }

        let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut stream = std::pin::pin!(sub.stream());
            let mut parked_tx = Some(parked_tx);
            std::future::poll_fn(move |cx| {
                let polled = stream.as_mut().poll_next(cx);
                if polled.is_pending() {
                    if let Some(tx) = parked_tx.take() {
                        let _ = tx.send(());
                    }
                }
                polled
            })
            .await
        });

        parked_rx.await.unwrap();
        seeker.seek(MemoryPosition::start()).await.unwrap();
        let replayed = timeout(Duration::from_secs(5), handle)
            .await
            .expect("a seek must wake the parked stream, not wait for the next publish")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed.payload(), b"a");
        replayed.ack().await.unwrap();
    }

    #[tokio::test]
    async fn batches_drop_stale_requeues_after_a_seek() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("seek.batch.stale");
        sub.set_batch_limit(2);
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.batch.stale", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches());
        let mut batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 2);
        let held_b = batch.pop().unwrap();
        let held_a = batch.pop().unwrap();
        assert_eq!(held_a.payload(), b"a");
        assert_eq!(held_b.payload(), b"b");

        // Clamped to the log end: queued "c" is drained and nothing is replayed.
        seeker.seek(MemoryPosition::sequence(10)).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // A stale copy at the head of the queue: the first-element loop filters it out.
        held_b.nack(true).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // A stale copy behind a live delivery: the batch-fill loop filters it out.
        publisher
            .publish(OutgoingMessage::new("seek.batch.stale", b"d".as_slice()))
            .await
            .unwrap();
        held_a.nack(true).await.unwrap();
        let live = stream.next().await.unwrap().unwrap();
        let payloads: Vec<&[u8]> = live.iter().map(IncomingMessage::payload).collect();
        assert_eq!(payloads, [b"d".as_slice()]);
        for msg in live {
            msg.ack().await.unwrap();
        }
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn seek_keeps_the_coordinator_in_flight_count_balanced() {
        let broker = MemoryBroker::new();
        let connected = broker.connect().await.unwrap();
        let coordinator = Coordinator::new(64);
        connected.install_coordinator(coordinator.clone());

        // Subscribed after the install, so every delivery carries the coordinator.
        let mut sub = connected.subscribe("seek.balance").await.unwrap();
        let seeker = sub.seeker();
        let publisher = connected.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("seek.balance", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        // Held unsettled so its requeue can race the seek below.
        let held = stream.next().await.unwrap().unwrap();
        assert_eq!(held.payload(), b"a");

        // Drains queued "b" (one consumed) and replays the log suffix (one enqueued).
        seeker.seek(MemoryPosition::sequence(1)).await.unwrap();
        let replayed = stream.next().await.unwrap().unwrap();
        assert_eq!(replayed.payload(), b"b");
        replayed.ack().await.unwrap();

        // The stale requeue of "a" is dropped by the watermark filter (one consumed).
        held.nack(true).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // Every enqueue across publish, drain, replay, requeue, and filter is balanced, so
        // the harness's quiescence wait resolves instead of hanging.
        coordinator.drive().await.unwrap();
    }

    #[tokio::test]
    async fn seek_scope_is_one_subscriber_instance() {
        let broker = MemoryBroker::new();
        let mut seeking = broker.subscribe("seek.scope");
        let mut bystander = broker.subscribe("seek.scope");
        let seeker = seeking.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("seek.scope", payload.as_slice()))
                .await
                .unwrap();
        }

        let mut seeking_stream = std::pin::pin!(seeking.stream());
        let mut bystander_stream = std::pin::pin!(bystander.stream());
        for _ in 0..2 {
            let msg = seeking_stream.next().await.unwrap().unwrap();
            msg.ack().await.unwrap();
            let msg = bystander_stream.next().await.unwrap().unwrap();
            msg.ack().await.unwrap();
        }

        seeker.seek(MemoryPosition::start()).await.unwrap();
        let replayed = seeking_stream.next().await.unwrap().unwrap();
        assert_eq!(replayed.payload(), b"a");
        replayed.ack().await.unwrap();
        let tail = seeking_stream.next().await.unwrap().unwrap();
        assert_eq!(tail.payload(), b"b");
        tail.ack().await.unwrap();

        // The sibling subscription of the same name is unaffected by the replay.
        assert!(futures::poll!(bystander_stream.next()).is_pending());
    }

    #[tokio::test]
    async fn partition_key_reads_well_known_header() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("keyed");
        let publisher = broker.publisher();

        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, b"user-42".as_slice());
        publisher
            .publish(OutgoingMessage::new("keyed", b"a".as_slice()).with_headers(headers))
            .await
            .unwrap();
        publisher
            .publish(OutgoingMessage::new("keyed", b"b".as_slice()))
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let keyed = stream.next().await.unwrap().unwrap();
        assert_eq!(
            Partitioned::partition_key(&keyed),
            Some(b"user-42".as_slice())
        );
        // The IncomingMessage hook must agree with the capability trait.
        assert_eq!(
            IncomingMessage::partition_key(&keyed),
            Some(b"user-42".as_slice())
        );
        keyed.ack().await.unwrap();

        let unkeyed = stream.next().await.unwrap().unwrap();
        assert_eq!(Partitioned::partition_key(&unkeyed), None);
        unkeyed.ack().await.unwrap();
    }
}
