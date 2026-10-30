//! Transactional publishers whose commit or shutdown is broken.

use std::{
    future::ready,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::time::sleep;

use super::{LOST_AFTER, on_current_thread_runtime};
use crate::capabilities::{owned_transactions, transactions};
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublisher, MemorySource,
};
use ruststream_core::{
    BytesMut, HeaderMap, OutgoingMessage, OwnedTransactions, Publisher, Take, Transaction,
    TransactionalPublisher,
};

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
    /// A transaction opened once the handle has met the shut-down bus fails to open. The honest
    /// answer of a client whose transactions need the connection.
    RefusesLateTransaction,
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
        let opened_after_shutdown = self.seen_shutdown.load(Ordering::SeqCst);
        if matches!(self.fault, TxFault::RefusesLateTransaction) && opened_after_shutdown {
            return ready(Err(MemoryError::ShutDown));
        }
        ready(Ok(FaultyTransaction {
            inner: self.inner.clone(),
            buffer: Vec::new(),
            fault: self.fault,
            opened_after_shutdown,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_transactions_pass_a_transaction_refused_after_shutdown() {
    owned_transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyOwnedPublisher::new(broker, TxFault::RefusesLateTransaction),
    )
    .await;
}
