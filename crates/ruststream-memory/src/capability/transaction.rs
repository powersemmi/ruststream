//! In-process transactions: a publisher handle buffers while one is open and fans the buffer
//! out on commit.

use std::mem;
use std::{fmt, future::ready, sync::Arc};

use tracing::warn;

use crate::{MemoryError, MemoryOutbound, MemoryPublisher, MemoryState};
use ruststream_core::{
    BytesMut, OutgoingMessage, OwnedTransactions, Take, Transaction, TransactionalPublisher,
};

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
            if let Err(err) = self
                .state
                .fanout(&outbound.name, outbound.payload, outbound.headers)
            {
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
/// [`transaction`](ruststream_core::OwnedTransactions::transaction) on a [`MemoryPublisher`].
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
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "ledger.settled")]
/// struct Settled {
///     id: u64,
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(Settled)]
/// struct Ledger;
///
/// #[subscriber("orders")]
/// async fn settle(
///     order: &Order,
///     Out(ledger): Out<impl OwnedTransactions, Ledger, Settled>,
/// ) -> HandlerOutcome {
///     // Nothing reaches `ledger.settled` until the commit, and an abort discards it all.
///     let Ok(mut txn) = ledger.transaction().await else {
///         return HandlerOutcome::retry();
///     };
///     if txn
///         .message(&Settled { id: order.id })
///         .publish()
///         .await
///         .is_err()
///         || txn.commit().await.is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(settle).out(Ledger, Publish)
///     })
/// }
/// # }
/// # fn main() {}
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
    // The buffer holds the messages until the commit fans them out, so it keeps what it is
    // handed.
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    fn publish(
        &mut self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), MemoryError>> {
        // Buffering is local to this value and never touches the bus; a commit against a
        // shut-down bus is what reports the error.
        self.buffered.push(MemoryOutbound::from(msg));
        ready(Ok(()))
    }

    fn commit(mut self) -> impl Future<Output = Result<(), MemoryError>> {
        // Settled before the flush: a failed commit has still consumed the transaction (the
        // buffer is lost per the Transaction contract), so the drop warning must not fire.
        self.settled = true;
        for outbound in mem::take(&mut self.buffered) {
            if let Err(err) = self
                .state
                .fanout(&outbound.name, outbound.payload, outbound.headers)
            {
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::MemoryBroker;
    use ruststream_core::{Broker, ConnectedBroker, IncomingMessage, Publisher, Subscriber};

    #[tokio::test]
    async fn clone_does_not_join_transaction() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("txn.clone");
        let transactional = broker.publisher();

        transactional.begin_transaction().await.unwrap();
        transactional
            .publish(OutgoingMessage::new("txn.clone", b"buffered"), None)
            .await
            .unwrap();

        let independent = transactional.clone();
        independent
            .publish(OutgoingMessage::new("txn.clone", b"direct"), None)
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
            .publish(OutgoingMessage::new("txn.down", b"buffered"), None)
            .await
            .unwrap();
        let connected = broker.connect().await.unwrap();
        connected.shutdown().await.unwrap();

        // Buffering never touched the bus, so the shutdown surfaces at the visibility point.
        assert_eq!(publisher.commit().await, Err(MemoryError::ShutDown));
    }

    #[tokio::test]
    async fn an_owned_transaction_dropped_unsettled_discards_its_buffer() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("orders");
        let publisher = broker.publisher();

        let mut transaction = publisher.transaction().await.unwrap();
        transaction
            .publish(OutgoingMessage::new("orders", b"buffered".as_slice()), None)
            .await
            .unwrap();
        assert!(format!("{transaction:?}").contains("buffered: 1"));

        // Dropping without commit or abort is an implicit abort: nothing becomes visible.
        drop(transaction);
        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(futures::poll!(stream.next()).is_pending());
    }
}
