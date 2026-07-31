//! Optional conformance suites, one per capability trait.
//!
//! A broker crate that implements a capability ([`RequestReply`], [`BatchSubscriber`],
//! [`TransactionalPublisher`], [`Seekable`]) runs the matching suite to prove the
//! implementation honours the trait contract; brokers without the capability simply do not call
//! it. Like [`harness::lifecycle`](super::harness::lifecycle), every suite is built from
//! caller-supplied factories so it stays broker-agnostic, and the in-memory broker is the
//! executable reference that passes all of them.

use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;

use super::harness::{expect_next, expect_no_more};
use crate::{
    AckError, BatchSubscriber, Broker, Connected, ConnectedBroker, Headers, IncomingMessage,
    OutgoingMessage, Positioned, Publisher, RequestReply, Seekable, Seeker, Subscriber,
    SubscriptionSource, TransactionalPublisher,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const MISS_TIMEOUT: Duration = Duration::from_millis(100);

/// Verifies the [`RequestReply`] contract.
///
/// A request reaches a responder with a usable `reply-to` header, the correlated reply resolves
/// the request, and a request nobody answers fails once its timeout elapses.
///
/// The factories mirror [`harness::lifecycle`](super::harness::lifecycle): `make_source` opens
/// the responder's subscription, `make_requester` produces the [`RequestReply`] publisher under
/// test, and `make_publisher` produces the plain publisher the responder replies through.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::request_reply(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     |broker| broker.requester(),
///     |broker| broker.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract.
pub async fn request_reply<B, MkBroker, Src, MkSrc, Req, MkReq, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_requester: MkReq,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Req: RequestReply,
    MkReq: Fn(&Connected<B>) -> Req,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const SUBJECT: &str = "conformance.request_reply";

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut responder = make_source(SUBJECT)
        .subscribe(&connected)
        .await
        .expect("responder subscription must open after connect");
    let publisher = make_publisher(&connected);
    let requester = make_requester(&connected);

    let respond = async {
        let mut stream = std::pin::pin!(responder.stream());
        let msg = expect_next(&mut stream, "request_reply responder").await;
        assert_eq!(
            msg.payload(),
            b"ping",
            "responder must receive the request payload"
        );
        let reply_to = msg
            .headers()
            .reply_to()
            .expect("a request must carry a usable reply-to header")
            .to_owned();

        // Echo the correlation id when the requester set one; replies must at minimum go to
        // the reply-to destination.
        let mut headers = Headers::new();
        if let Some(correlation_id) = msg.headers().correlation_id() {
            headers.insert("correlation-id", correlation_id.to_owned());
        }
        publisher
            .publish(OutgoingMessage::new(&reply_to, b"pong".as_slice()).with_headers(headers))
            .await
            .expect("reply publish failed");
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    };
    let request = requester.request(
        OutgoingMessage::new(SUBJECT, b"ping".as_slice()),
        DEFAULT_TIMEOUT,
    );

    let (reply, ()) = futures::join!(request, respond);
    let reply = reply.expect("request must resolve once the responder replies");
    assert_eq!(
        reply.payload(),
        b"pong",
        "the correlated reply must carry the responder payload"
    );

    let unanswered = requester
        .request(
            OutgoingMessage::new("conformance.request_reply.void", b"ping".as_slice()),
            MISS_TIMEOUT,
        )
        .await;
    assert!(
        unanswered.is_err(),
        "a request nobody answers must fail once its timeout elapses",
    );

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// Verifies the [`BatchSubscriber`] contract.
///
/// Every published message arrives, in publish order, distributed over one or more non-empty
/// batches.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::batches(
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
pub async fn batches<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: BatchSubscriber + Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const SUBJECT: &str = "conformance.batches";
    const COUNT: u32 = 10;

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(SUBJECT)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    let publisher = make_publisher(&connected);

    for i in 0..COUNT {
        publisher
            .publish(OutgoingMessage::new(SUBJECT, i.to_be_bytes().as_slice()))
            .await
            .expect("publish failed");
    }

    let mut received = Vec::new();
    let mut stream = std::pin::pin!(subscriber.batches());
    while received.len() < COUNT as usize {
        let batch = timeout(DEFAULT_TIMEOUT, stream.next())
            .await
            .expect("batches: stream timed out")
            .expect("batches: stream ended unexpectedly")
            .unwrap_or_else(|err| panic!("batches: stream yielded error: {err:?}"));

        let batch: Vec<_> = batch.into_iter().collect();
        assert!(!batch.is_empty(), "a yielded batch must not be empty");
        for msg in batch {
            received.push(msg.payload().to_vec());
            match msg.ack().await {
                Ok(()) | Err(AckError::Unsupported) => {}
                Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
            }
        }
    }

    let expected: Vec<Vec<u8>> = (0..COUNT).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_eq!(
        received, expected,
        "batched deliveries must preserve publish order across batches",
    );

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// Verifies the [`TransactionalPublisher`] contract.
///
/// Nothing published inside a transaction is visible before `commit`, a commit makes every
/// buffered message visible in publish order, and an abort discards the buffer. Misuse must
/// error rather than silently succeed: `commit` / `abort` with no open transaction, and a
/// second `begin_transaction` while one is open (which must also leave the open transaction
/// untouched).
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
    Pub: TransactionalPublisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const SUBJECT: &str = "conformance.transactions";

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(SUBJECT)
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
        .publish(OutgoingMessage::new(SUBJECT, b"first".as_slice()))
        .await
        .expect("publish inside transaction failed");
    publisher
        .publish(OutgoingMessage::new(SUBJECT, b"second".as_slice()))
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
        .publish(OutgoingMessage::new(SUBJECT, b"discarded".as_slice()))
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
        .publish(OutgoingMessage::new(SUBJECT, b"third".as_slice()))
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

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// Verifies the [`Seekable`] contract.
///
/// Positions captured from delivered messages ([`Positioned::position`]) follow the pinned
/// semantics: a seek back to a captured position redelivers exactly that message and the suffix
/// after it in order, a seek forward past queued deliveries skips them, and the subscription
/// keeps delivering new publishes after repositioning. The seeker is minted before the stream
/// borrows the subscriber, which is the shape the capability exists for.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::seeking(
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
pub async fn seeking<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Seekable + Send,
    <Src::Subscriber as Subscriber>::Message:
        Positioned<Position = <<Src::Subscriber as Seekable>::Seeker as Seeker>::Position>,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const SUBJECT: &str = "conformance.seeking";
    const COUNT: u8 = 5;

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(SUBJECT)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    // Minted before `stream` borrows the subscriber; usable while the stream runs.
    let seeker = subscriber.seeker();
    let publisher = make_publisher(&connected);

    for i in 0..COUNT {
        publisher
            .publish(OutgoingMessage::new(SUBJECT, &[i]))
            .await
            .expect("publish failed");
    }

    let mut stream = std::pin::pin!(subscriber.stream());
    let mut positions = Vec::new();
    for i in 0..COUNT {
        let msg = expect_next(&mut stream, "seeking: initial delivery").await;
        assert_eq!(
            msg.payload(),
            &[i],
            "initial deliveries must arrive in publish order",
        );
        positions.push(msg.position());
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    expect_no_more(&mut stream, "seeking: after the initial drain").await;

    // Extraction order matters: swap_remove(4) pops the last element, which leaves the value
    // at index 1 in place for the second extraction.
    let forward_to = positions.swap_remove(4);
    let back_to = positions.swap_remove(1);

    seeker.seek(back_to).await.expect("seek back failed");
    let redelivered = expect_next(&mut stream, "seeking: after the seek back").await;
    assert_eq!(
        redelivered.payload(),
        &[1],
        "a seek back must redeliver the message at the captured position",
    );
    match redelivered.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    // One more delivery pins the suffix: replaying only the sought message is not enough.
    let suffix = expect_next(&mut stream, "seeking: suffix after the seek back").await;
    assert_eq!(
        suffix.payload(),
        &[2],
        "a seek back must redeliver the ordered suffix after the captured position",
    );
    match suffix.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    // Delivery 3 is pending now; jumping to the captured position of 4 must skip it.
    seeker.seek(forward_to).await.expect("seek forward failed");
    let skipped_to = expect_next(&mut stream, "seeking: after the seek forward").await;
    assert_eq!(
        skipped_to.payload(),
        &[4],
        "a seek forward must skip the queued deliveries before the target",
    );
    match skipped_to.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    expect_no_more(&mut stream, "seeking: after the forward target").await;

    publisher
        .publish(OutgoingMessage::new(SUBJECT, &[COUNT]))
        .await
        .expect("publish failed");
    let live = expect_next(&mut stream, "seeking: after a new publish").await;
    assert_eq!(
        live.payload(),
        &[COUNT],
        "the subscription must keep delivering new publishes after repositioning",
    );
    match live.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}
