//! The transaction suites, for the borrowed and the owned kind.

use std::fmt;

use futures::Stream;

use super::{collect_payloads, collect_payloads_within, promptly};
use crate::conformance::harness::{expect_next, expect_no_more, on_foreign_runtime};
use crate::conformance::helpers::unique_subject;
use crate::{
    AckError, Broker, Connected, ConnectedBroker, IncomingMessage, OutgoingMessage,
    OwnedTransactions, Subscriber, SubscriptionSource, Transaction, TransactionalPublisher,
};

/// Verifies the [`TransactionalPublisher`] contract.
///
/// Nothing published inside a transaction is visible before `commit`, a commit makes every
/// buffered message visible in publish order, and an abort discards the buffer. Misuse must
/// error rather than silently succeed: `commit` / `abort` with no open transaction, and a
/// second `begin_transaction` while one is open (which must also leave the open transaction
/// untouched).
///
/// A transaction begun, filled and committed from a current-thread runtime that stops right after
/// (a handler on a dedicated thread) is published all the same: what the commit leaves to finish
/// runs on the runtime the broker connected on. Through a publisher that outlived the shutdown, a
/// commit of the transaction left open and a plain publish both return an error, because a
/// success there would report messages that never reached the broker. The publisher moves to the
/// other thread, so it is `'static`.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::transactions(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     |broker| broker.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract.
pub async fn transactions<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: TransactionalPublisher + 'static,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.transactions");

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    let publisher = make_publisher(&connected);
    let mut stream = std::pin::pin!(subscriber.stream());

    publisher
        .begin_transaction()
        .await
        .expect("begin_transaction failed");
    publisher
        .publish(OutgoingMessage::new(&subject, b"first".as_slice()), None)
        .await
        .expect("publish inside transaction failed");
    publisher
        .publish(OutgoingMessage::new(&subject, b"second".as_slice()), None)
        .await
        .expect("publish inside transaction failed");
    expect_no_more(&mut stream, "transactions: before commit").await;

    publisher.commit().await.expect("commit failed");
    let first = expect_next(&mut stream, "transactions: first after commit").await;
    assert_eq!(
        first.payload(),
        b"first",
        "commit must make buffered messages visible in publish order",
    );
    match first.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    let second = expect_next(&mut stream, "transactions: second after commit").await;
    assert_eq!(second.payload(), b"second");
    match second.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    publisher
        .begin_transaction()
        .await
        .expect("begin_transaction failed");
    publisher
        .publish(
            OutgoingMessage::new(&subject, b"discarded".as_slice()),
            None,
        )
        .await
        .expect("publish inside transaction failed");
    publisher.abort().await.expect("abort failed");
    expect_no_more(&mut stream, "transactions: after abort").await;

    // Misuse must surface as errors, never as silent success.
    assert!(
        publisher.commit().await.is_err(),
        "commit with no open transaction must error",
    );
    assert!(
        publisher.abort().await.is_err(),
        "abort with no open transaction must error",
    );
    publisher
        .begin_transaction()
        .await
        .expect("begin_transaction failed");
    assert!(
        publisher.begin_transaction().await.is_err(),
        "begin_transaction while a transaction is open must error",
    );
    // The rejected second begin must not have disturbed the open transaction.
    publisher
        .publish(OutgoingMessage::new(&subject, b"third".as_slice()), None)
        .await
        .expect("publish inside transaction failed");
    publisher
        .commit()
        .await
        .expect("commit after a rejected double begin failed");
    let third = expect_next(&mut stream, "transactions: after rejected double begin").await;
    assert_eq!(
        third.payload(),
        b"third",
        "a rejected double begin must leave the open transaction intact",
    );
    match third.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    let publisher = transaction_commit_on_foreign_runtime(publisher, &mut stream, &subject).await;
    transaction_after_shutdown(connected, &publisher, &subject).await;
}

/// Begun, filled and committed from a runtime that stops right after: the buffer is published
/// all the same.
async fn transaction_commit_on_foreign_runtime<Pub, S, M, E>(
    publisher: Pub,
    stream: &mut S,
    subject: &str,
) -> Pub
where
    Pub: TransactionalPublisher + 'static,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    // The way a handler on a dedicated thread commits.
    let destination = subject.to_owned();
    let publisher = on_foreign_runtime(async move || {
        publisher
            .begin_transaction()
            .await
            .expect("transactions: begin_transaction from another runtime failed");
        for payload in [b"foreign-1".as_slice(), b"foreign-2"] {
            publisher
                .publish(OutgoingMessage::new(&destination, payload), None)
                .await
                .expect("transactions: publish inside a transaction from another runtime failed");
        }
        publisher
            .commit()
            .await
            .expect("transactions: commit from another runtime failed");
        publisher
    })
    .await;
    assert_eq!(
        collect_payloads_within(
            stream,
            2,
            "transactions: after a commit from a runtime that has since stopped",
            "a commit made from a runtime that has since stopped must still publish; the broker \
             must run its internal tasks on the runtime it connected on",
        )
        .await,
        [b"foreign-1".to_vec(), b"foreign-2".to_vec()],
        "transactions: a commit from another runtime must publish the buffer in order",
    );
    publisher
}

/// A transaction left open across the shutdown: its commit and a plain publish after it both
/// return an error.
async fn transaction_after_shutdown<C, Pub>(connected: C, publisher: &Pub, subject: &str)
where
    C: ConnectedBroker,
    Pub: TransactionalPublisher,
{
    publisher
        .begin_transaction()
        .await
        .expect("begin_transaction failed");
    publisher
        .publish(OutgoingMessage::new(subject, b"stranded".as_slice()), None)
        .await
        .expect("publish inside transaction failed");

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");

    let commit = promptly(publisher.commit(), "transactions: a commit after shutdown").await;
    assert!(
        commit.is_err(),
        "transactions: a commit after shutdown reported success; it must return an error, since \
         the buffered messages never reached the broker",
    );
    let direct = promptly(
        publisher.publish(OutgoingMessage::new(subject, b"after".as_slice()), None),
        "transactions: a publish after shutdown",
    )
    .await;
    assert!(
        direct.is_err(),
        "transactions: a publish after shutdown reported success; it must return an error",
    );
}

/// Verifies the [`OwnedTransactions`] / [`Transaction`] contract.
///
/// Nothing published into an open transaction is visible before `commit`, a commit makes the
/// whole buffer visible atomically in publish order, and an abort discards it. Transactions are
/// independent: two open at once on one handle settle without affecting each other, and the
/// handle keeps publishing directly while one is open. That a settled transaction cannot be
/// reused is enforced by the consuming `commit` / `abort` at compile time, so this suite does not
/// assert it.
///
/// A transaction opened, filled and committed from a current-thread runtime that stops right
/// after is published all the same. After the shutdown, a commit of a transaction opened before
/// it, a direct publish and a transaction opened after it all return an error, at the latest when
/// the commit is attempted. The publisher moves to the other thread, so it is `'static`.
///
/// The suite makes no claim about the delivery order between two transactions - only about the
/// order inside each committed buffer.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::owned_transactions(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     |broker| broker.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract.
pub async fn owned_transactions<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: OwnedTransactions + 'static,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.owned_transactions");

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    let publisher = make_publisher(&connected);
    let mut stream = std::pin::pin!(subscriber.stream());

    owned_settlement(&publisher, &mut stream, &subject).await;
    owned_concurrent_settlements(&publisher, &mut stream, &subject).await;
    owned_concurrent_commits(&publisher, &mut stream, &subject).await;
    owned_direct_publish(&publisher, &mut stream, &subject).await;
    let publisher = owned_commit_on_foreign_runtime(publisher, &mut stream, &subject).await;

    let mut stranded = publisher
        .transaction()
        .await
        .expect("transaction must open");
    stranded
        .publish(OutgoingMessage::new(&subject, b"stranded".as_slice()), None)
        .await
        .expect("publish into a transaction failed");

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");

    owned_after_shutdown(&publisher, stranded, &subject).await;
}

/// Opened, filled and committed from a runtime that stops right after, the way a handler on a
/// dedicated thread commits: the buffer is published all the same.
async fn owned_commit_on_foreign_runtime<Pub, S, M, E>(
    publisher: Pub,
    stream: &mut S,
    subject: &str,
) -> Pub
where
    Pub: OwnedTransactions + 'static,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let destination = subject.to_owned();
    let publisher = on_foreign_runtime(async move || {
        let mut transaction = publisher
            .transaction()
            .await
            .expect("owned_transactions: a transaction must open from another runtime");
        for payload in [b"foreign-1".as_slice(), b"foreign-2"] {
            transaction
                .publish(OutgoingMessage::new(&destination, payload), None)
                .await
                .expect(
                    "owned_transactions: publish into a transaction from another runtime failed",
                );
        }
        transaction
            .commit()
            .await
            .expect("owned_transactions: commit from another runtime failed");
        publisher
    })
    .await;
    assert_eq!(
        collect_payloads_within(
            stream,
            2,
            "owned_transactions: after a commit from a runtime that has since stopped",
            "a commit made from a runtime that has since stopped must still publish; the broker \
             must run its internal tasks on the runtime it connected on",
        )
        .await,
        [b"foreign-1".to_vec(), b"foreign-2".to_vec()],
        "owned_transactions: a commit from another runtime must publish the buffer in order",
    );
    publisher
}

/// After the shutdown nothing reports a publish that never reached the broker: the commit of a
/// transaction opened before it, a direct publish, and a transaction opened after it.
async fn owned_after_shutdown<Pub>(publisher: &Pub, stranded: Pub::Transaction, subject: &str)
where
    Pub: OwnedTransactions,
{
    let commit = promptly(
        stranded.commit(),
        "owned_transactions: a commit after shutdown",
    )
    .await;
    assert!(
        commit.is_err(),
        "owned_transactions: the commit of a transaction opened before the shutdown reported \
         success after it; it must return an error, since the buffer never reached the broker",
    );
    let direct = promptly(
        publisher.publish(OutgoingMessage::new(subject, b"after".as_slice()), None),
        "owned_transactions: a direct publish after shutdown",
    )
    .await;
    assert!(
        direct.is_err(),
        "owned_transactions: a direct publish after shutdown reported success; it must return an \
         error",
    );
    // A client buffer may open and fill without the connection; the commit is the visibility
    // point, and it is where a dead connection has to surface at the latest.
    if let Ok(mut late) = promptly(
        publisher.transaction(),
        "owned_transactions: a transaction opened after shutdown",
    )
    .await
    {
        let _ = promptly(
            late.publish(OutgoingMessage::new(subject, b"late".as_slice()), None),
            "owned_transactions: a publish into a transaction opened after shutdown",
        )
        .await;
        let commit = promptly(
            late.commit(),
            "owned_transactions: the commit of a transaction opened after shutdown",
        )
        .await;
        assert!(
            commit.is_err(),
            "owned_transactions: a transaction opened after the shutdown committed with success; \
             opening or committing it must return an error",
        );
    }
}

/// One transaction at a time: a commit publishes the whole buffer in order, an abort discards it.
async fn owned_settlement<Pub, S, M, E>(publisher: &Pub, stream: &mut S, subject: &str)
where
    Pub: OwnedTransactions,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut committed = publisher
        .transaction()
        .await
        .expect("transaction must open");
    committed
        .publish(OutgoingMessage::new(subject, b"first".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    committed
        .publish(OutgoingMessage::new(subject, b"second".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    expect_no_more(stream, "owned_transactions: before commit").await;

    committed.commit().await.expect("commit failed");
    assert_eq!(
        collect_payloads(stream, 2, "owned_transactions: after commit").await,
        vec![b"first".to_vec(), b"second".to_vec()],
        "commit must make the whole buffer visible in publish order",
    );

    let mut aborted = publisher
        .transaction()
        .await
        .expect("transaction must open");
    aborted
        .publish(OutgoingMessage::new(subject, b"discarded".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    aborted.abort().await.expect("abort failed");
    expect_no_more(stream, "owned_transactions: after abort").await;
}

/// Two transactions open at once on one handle, settled the opposite ways: only the committed
/// buffer arrives, and its sibling's abort leaves it whole.
async fn owned_concurrent_settlements<Pub, S, M, E>(publisher: &Pub, stream: &mut S, subject: &str)
where
    Pub: OwnedTransactions,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut kept = publisher
        .transaction()
        .await
        .expect("transaction must open");
    let mut dropped = publisher
        .transaction()
        .await
        .expect("a second transaction must open while the first is open");
    kept.publish(OutgoingMessage::new(subject, b"kept-1".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    dropped
        .publish(OutgoingMessage::new(subject, b"dropped-1".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    kept.publish(OutgoingMessage::new(subject, b"kept-2".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    dropped
        .publish(OutgoingMessage::new(subject, b"dropped-2".as_slice()), None)
        .await
        .expect("publish into a transaction failed");

    dropped.abort().await.expect("abort failed");
    expect_no_more(stream, "owned_transactions: sibling aborted").await;

    kept.commit().await.expect("commit failed");
    assert_eq!(
        collect_payloads(stream, 2, "owned_transactions: sibling committed").await,
        vec![b"kept-1".to_vec(), b"kept-2".to_vec()],
        "aborting one transaction must leave a concurrent one whole and in publish order",
    );
    expect_no_more(stream, "owned_transactions: after the concurrent pair").await;
}

/// Two transactions open at once, both committed: each buffer arrives whole and in publish order.
async fn owned_concurrent_commits<Pub, S, M, E>(publisher: &Pub, stream: &mut S, subject: &str)
where
    Pub: OwnedTransactions,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut left = publisher
        .transaction()
        .await
        .expect("transaction must open");
    let mut right = publisher
        .transaction()
        .await
        .expect("a second transaction must open while the first is open");
    left.publish(OutgoingMessage::new(subject, b"left-1".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    right
        .publish(OutgoingMessage::new(subject, b"right-1".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    left.publish(OutgoingMessage::new(subject, b"left-2".as_slice()), None)
        .await
        .expect("publish into a transaction failed");

    left.commit().await.expect("commit failed");
    right.commit().await.expect("commit failed");

    let both = collect_payloads(stream, 3, "owned_transactions: both committed").await;
    let from_left: Vec<Vec<u8>> = both
        .iter()
        .filter(|payload| payload.starts_with(b"left"))
        .cloned()
        .collect();
    assert_eq!(
        from_left,
        vec![b"left-1".to_vec(), b"left-2".to_vec()],
        "each committed buffer must arrive whole, in publish order",
    );
    assert!(
        both.iter().any(|payload| payload == b"right-1"),
        "committing two concurrent transactions must deliver both buffers",
    );
}

/// The handle keeps publishing directly while a transaction is open, and neither captures the
/// other's messages.
async fn owned_direct_publish<Pub, S, M, E>(publisher: &Pub, stream: &mut S, subject: &str)
where
    Pub: OwnedTransactions,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut open = publisher
        .transaction()
        .await
        .expect("transaction must open");
    open.publish(OutgoingMessage::new(subject, b"buffered".as_slice()), None)
        .await
        .expect("publish into a transaction failed");
    publisher
        .publish(OutgoingMessage::new(subject, b"direct".as_slice()), None)
        .await
        .expect("the handle must keep publishing directly while a transaction is open");
    assert_eq!(
        collect_payloads(stream, 1, "owned_transactions: direct publish").await,
        vec![b"direct".to_vec()],
        "a direct publish must be visible immediately, not buffered by an open transaction",
    );

    open.commit().await.expect("commit failed");
    assert_eq!(
        collect_payloads(stream, 1, "owned_transactions: after the direct publish").await,
        vec![b"buffered".to_vec()],
        "a direct publish must leave the open transaction's buffer intact",
    );
}
