//! Native capability-trait implementations for the in-memory broker.
//!
//! These are first-class features of the broker's own in-process semantics, not simulations of
//! another broker: real buffered transactions, real greedy batching, real correlated request /
//! reply over an in-process inbox. The conformance harness offers a suite per capability (see
//! `conformance::capabilities`), and these implementations are the executable reference for
//! them.

use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
    time::Duration,
};

use bytes::Bytes;
use futures::Stream;
use thiserror::Error;
use tokio::{sync::mpsc, time::timeout};

use super::{
    MemoryDelivery, MemoryError, MemoryMessage, MemoryPublisher, MemoryState, MemorySubscriber,
};
use crate::{
    BatchSubscriber, IncomingMessage, OutgoingMessage, Partitioned, Publisher, RequestReply,
    Subscriber, TransactionalPublisher,
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

impl std::fmt::Debug for MemoryRequester {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryRequester").finish_non_exhaustive()
    }
}

impl Publisher for MemoryRequester {
    type Error = RequestError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let delivery = MemoryDelivery {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers: msg.headers().clone(),
        };
        self.state
            .fanout(&delivery)
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
        let delivery = MemoryDelivery {
            name: msg.name().to_owned(),
            payload: Bytes::copy_from_slice(msg.payload()),
            headers,
        };
        if self.state.fanout(&delivery).is_err() {
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
            let Some(first) = std::task::ready!(self.rx.poll_recv(cx)) else {
                return Poll::Ready(None);
            };
            let mut batch = vec![MemoryMessage {
                delivery: Some(first),
                requeue: requeue.clone(),
                #[cfg(feature = "testing")]
                coordinator: coordinator.clone(),
            }];
            while batch.len() < limit {
                match self.rx.poll_recv(cx) {
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
        for delivery in buffered {
            self.state.fanout(&delivery)?;
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

/// The partition key is carried in the [`PARTITION_KEY_HEADER`] header; producers that want
/// per-key ordering set it at publish time. Messages without the header return `None` (any
/// partition will do).
impl Partitioned for MemoryMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::super::MemoryBroker;
    use super::*;
    use crate::{Broker, ConnectedBroker, Headers};

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
            super::super::Bus::Live(subscribers) => {
                subscribers.keys().any(|name| name.starts_with("_inbox."))
            }
            super::super::Bus::ShutDown => false,
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
            super::super::Bus::Live(subscribers) => {
                subscribers.keys().any(|name| name.starts_with("_inbox."))
            }
            super::super::Bus::ShutDown => false,
        };
        assert!(!inbox_leaked);
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
