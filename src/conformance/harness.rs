//! Conformance test suite that any [`TestableBroker`] implementation must pass.
//!
//! Broker authors prove their in-process transport honours Core routing by running the suite
//! against the [`TestableBroker`] their crate ships under the `testing` feature. Each test starts
//! from a fresh broker produced by the caller-supplied factory and drives it through the broker's
//! own [`Subscribe`] / [`TestableBroker::inject`] surface - no server.
//!
//! # Examples
//!
//! The example uses [`crate::memory::MemoryBroker`] as a stand-in broker, so it needs the
//! `memory` feature; a broker crate substitutes its own in-process transport here.
//!
//! ```no_run
//! # #[cfg(all(feature = "testing", feature = "memory"))]
//! # async fn run() {
//! use ruststream::{conformance::harness, memory::MemoryBroker};
//!
//! harness::run_suite(MemoryBroker::new).await;
//! # }
//! ```

use std::time::Duration;

use crate::{
    AckError, Broker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscribe, Subscriber,
    SubscriptionSource, testing::TestableBroker,
};
use bytes::Bytes;
use futures::StreamExt;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const NEGATIVE_WAIT: Duration = Duration::from_millis(100);

/// Runs every scenario in the suite, panicking with a descriptive message on the first failure.
///
/// `factory` is invoked once per scenario to obtain a fresh broker, so tests cannot leak state
/// between each other.
///
/// # Panics
///
/// Panics if any scenario fails an assertion. The panic message identifies the scenario.
pub async fn run_suite<B, F>(factory: F)
where
    B: TestableBroker + Subscribe,
    F: Fn() -> B,
{
    ordering(factory()).await;
    publish_after_subscribe(factory()).await;
    ack_consumes_delivery(factory()).await;
    nack_with_requeue_redelivers(factory()).await;
    nack_without_requeue_drops(factory()).await;
    headers_propagate(factory()).await;
    published_log_observes_publishes(factory()).await;
}

/// Verifies a broker honours the lazy-startup contract end to end.
///
/// The steps are: synchronous construction (no I/O in the constructor), then `connect`, a
/// subscription opened through the broker's own [`SubscriptionSource`], a publish the subscription
/// receives and acks (or reports [`AckError::Unsupported`] for a broker with no ack semantics),
/// then `shutdown` - and the post-shutdown contract: publish and subscribe must error against the
/// shut-down broker, a second `shutdown` stays `Ok`, and a later `connect` either revives the
/// broker (proved by a working subscribe / publish / deliver round trip) or returns an error;
/// reporting `Ok` while staying dead is the one forbidden outcome.
///
/// The three factories keep the check broker-agnostic:
/// * `make_broker` is **synchronous** (`Fn() -> B`). A broker that can only be built asynchronously
///   cannot satisfy it, which is exactly the contract: construct cheaply, connect lazily in
///   [`Broker::connect`].
/// * `make_source` builds the broker's subscription descriptor for a subject (the macro-subscriber
///   path).
/// * `make_publisher` produces a publisher from the connected broker.
///
/// Run it from the broker crate, against a real server where one is needed (NATS, Kafka, ...) or
/// in-process for the in-memory broker.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::{conformance::harness, memory::{MemoryBroker, MemorySource}};
///
/// harness::lifecycle(
///     || MemoryBroker::new(),
///     |name| MemorySource::new(name),
///     |broker| broker.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if construction, connection, subscription, delivery, ack, or
/// shutdown does not behave as the contract requires.
pub async fn lifecycle<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<B> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&B) -> Pub,
{
    const SUBJECT: &str = "conformance.lifecycle";

    let broker = make_broker();
    Broker::connect(&broker)
        .await
        .expect("broker must connect after synchronous construction");

    let mut subscriber = make_source(SUBJECT)
        .subscribe(&broker)
        .await
        .expect("subscription source must open after connect");
    let publisher = make_publisher(&broker);

    publisher
        .publish(OutgoingMessage::new(SUBJECT, b"lifecycle".as_slice()))
        .await
        .expect("publish after connect failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "lifecycle").await;
    assert_eq!(
        msg.payload(),
        b"lifecycle",
        "subscription opened through SubscriptionSource must receive the publish",
    );
    // Ack must either succeed or be explicitly unsupported (a broker with no ack semantics, e.g.
    // Core NATS). Any other ack error is a real failure.
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    Broker::shutdown(&broker)
        .await
        .expect("broker must shut down cleanly");

    // Shutdown is a state, not an event: operations against the shut-down broker must error
    // rather than silently succeed against a dead connection.
    assert!(
        publisher
            .publish(OutgoingMessage::new(SUBJECT, b"post-shutdown".as_slice()))
            .await
            .is_err(),
        "publish after shutdown must error",
    );
    assert!(
        make_source(SUBJECT).subscribe(&broker).await.is_err(),
        "subscribing after shutdown must error",
    );
    Broker::shutdown(&broker)
        .await
        .expect("shutdown must stay idempotent");

    // A post-shutdown connect either revives the broker or errors; Ok-but-dead is forbidden.
    // (An Err here is fine: reconnect-after-shutdown is not required, a clear error satisfies
    // the contract.)
    if Broker::connect(&broker).await.is_ok() {
        let mut subscriber = make_source(SUBJECT)
            .subscribe(&broker)
            .await
            .expect("subscribe must work after a reconnect that reported Ok");
        let publisher = make_publisher(&broker);
        publisher
            .publish(OutgoingMessage::new(SUBJECT, b"revived".as_slice()))
            .await
            .expect("publish must work after a reconnect that reported Ok");
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = expect_next(&mut stream, "lifecycle: after reconnect").await;
        assert_eq!(
            msg.payload(),
            b"revived",
            "a reconnect that reports Ok must actually deliver",
        );
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
        Broker::shutdown(&broker)
            .await
            .expect("broker must shut down cleanly");
    }
}

async fn ordering<B: TestableBroker + Subscribe>(broker: B) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.ordering")
        .await
        .expect("subscribe failed");

    for i in 0..10u32 {
        broker.inject(OutgoingMessage::new(
            "conformance.ordering",
            i.to_be_bytes().as_slice(),
        ));
    }

    let mut stream = std::pin::pin!(subscriber.stream());
    for expected in 0..10u32 {
        let msg = expect_next(&mut stream, "ordering").await;
        assert_eq!(
            msg.payload(),
            expected.to_be_bytes(),
            "messages must be delivered in publish order",
        );
        msg.ack().await.expect("ack failed");
    }
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

async fn publish_after_subscribe<B: TestableBroker + Subscribe>(broker: B) {
    broker.inject(OutgoingMessage::new(
        "conformance.late",
        b"before-subscribe".as_slice(),
    ));

    let mut subscriber = Subscribe::subscribe(&broker, "conformance.late")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new(
        "conformance.late",
        b"after-subscribe".as_slice(),
    ));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "publish_after_subscribe").await;
    assert_eq!(
        msg.payload(),
        b"after-subscribe",
        "subscriber must receive only messages published after subscription opened",
    );
    msg.ack().await.expect("ack failed");
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

async fn ack_consumes_delivery<B: TestableBroker + Subscribe>(broker: B) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.ack")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.ack", b"one".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "ack_consumes_delivery").await;
    msg.ack().await.expect("ack failed");

    expect_no_more(&mut stream, "ack_consumes_delivery").await;
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

async fn nack_with_requeue_redelivers<B: TestableBroker + Subscribe>(broker: B) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.requeue")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new(
        "conformance.requeue",
        b"retry-me".as_slice(),
    ));

    let mut stream = std::pin::pin!(subscriber.stream());
    let first = expect_next(&mut stream, "nack_with_requeue first").await;
    assert_eq!(first.payload(), b"retry-me");
    first.nack(true).await.expect("nack failed");

    let second = expect_next(&mut stream, "nack_with_requeue second").await;
    assert_eq!(
        second.payload(),
        b"retry-me",
        "nack(requeue=true) must redeliver the same payload",
    );
    second.ack().await.expect("ack failed");
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

async fn nack_without_requeue_drops<B: TestableBroker + Subscribe>(broker: B) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.drop")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.drop", b"gone".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "nack_without_requeue").await;
    msg.nack(false).await.expect("nack failed");

    expect_no_more(&mut stream, "nack_without_requeue").await;
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

async fn headers_propagate<B: TestableBroker + Subscribe>(broker: B) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.headers")
        .await
        .expect("subscribe failed");

    let mut headers = Headers::new();
    headers.insert("Content-Type", "application/json");
    headers.insert("X-Tenant", Bytes::from_static(b"acme"));

    broker.inject(
        OutgoingMessage::new("conformance.headers", b"{}".as_slice()).with_headers(headers),
    );

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "headers_propagate").await;
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().get("x-tenant"), Some(b"acme".as_slice()));
    msg.ack().await.expect("ack failed");
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

async fn published_log_observes_publishes<B: TestableBroker + Subscribe>(broker: B) {
    broker.inject(OutgoingMessage::new(
        "conformance.observe",
        b"first".as_slice(),
    ));
    broker.inject(OutgoingMessage::new(
        "conformance.observe",
        b"second".as_slice(),
    ));

    let observed = broker.published("conformance.observe");
    assert_eq!(
        observed.len(),
        2,
        "the publish log must observe every publish",
    );
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
    Broker::shutdown(&broker).await.expect("shutdown failed");
}

pub(crate) async fn expect_next<S, M, E>(stream: &mut S, label: &str) -> M
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: std::fmt::Debug,
{
    let item = tokio::time::timeout(DEFAULT_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: stream timed out"));
    let item = item.unwrap_or_else(|| panic!("{label}: stream ended unexpectedly"));
    item.unwrap_or_else(|err| panic!("{label}: stream yielded error: {err:?}"))
}

pub(crate) async fn expect_no_more<S, M, E>(stream: &mut S, label: &str)
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: std::fmt::Debug,
{
    let result = tokio::time::timeout(NEGATIVE_WAIT, stream.next()).await;
    assert!(
        result.is_err(),
        "{label}: expected no further deliveries within {NEGATIVE_WAIT:?}",
    );
}
