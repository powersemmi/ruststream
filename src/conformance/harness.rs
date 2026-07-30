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

use std::{fmt, time::Duration};

use crate::{
    AckError, Broker, Connected, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage,
    Publisher, Subscribe, Subscriber, SubscriptionSource, testing::TestableBroker,
};
use bytes::Bytes;
use futures::StreamExt;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const NEGATIVE_WAIT: Duration = Duration::from_millis(100);

/// Runs every scenario in the suite, panicking with a descriptive message on the first failure.
///
/// `factory` is invoked once per scenario to obtain a fresh broker, so tests cannot leak state
/// between each other. Each scenario connects the broker (the consuming ladder transition) and
/// drives the connected form through the broker's own [`Subscribe`] /
/// [`TestableBroker::inject`] surface.
///
/// # Panics
///
/// Panics if any scenario fails an assertion. The panic message identifies the scenario.
pub async fn run_suite<B, F>(factory: F)
where
    B: Broker,
    B::Connected: TestableBroker + Subscribe,
    F: Fn() -> B,
{
    let connect = async move |broker: B| {
        broker
            .connect()
            .await
            .expect("broker must connect before a suite scenario")
    };
    ordering(connect(factory()).await).await;
    publish_after_subscribe(connect(factory()).await).await;
    ack_consumes_delivery(connect(factory()).await).await;
    nack_with_requeue_redelivers(connect(factory()).await).await;
    nack_without_requeue_drops(connect(factory()).await).await;
    headers_propagate(connect(factory()).await).await;
    published_log_observes_publishes(connect(factory()).await).await;
}

/// Verifies a broker honours the lifecycle ladder end to end.
///
/// The steps are: synchronous construction (no I/O in the constructor), then the consuming
/// `connect` producing the typed connected form, a subscription opened through the broker's own
/// [`SubscriptionSource`], a publish the subscription receives and acks (or reports
/// [`AckError::Unsupported`] for a broker with no ack semantics), then the consuming `shutdown`
/// producing the terminal witness. Owner-side misuse after shutdown is a compile error under the
/// ladder, so what remains checkable at runtime is the aliased-handle contract: a publisher
/// created before the shutdown must error afterwards, never silently succeed against a dead
/// connection.
///
/// The three factories keep the check broker-agnostic:
/// * `make_broker` is **synchronous** (`Fn() -> B`). A broker that can only be built asynchronously
///   cannot satisfy it, which is exactly the contract: construct cheaply, connect in
///   [`Broker::connect`].
/// * `make_source` builds the broker's subscription descriptor for a subject (the macro-subscriber
///   path).
/// * `make_publisher` produces a publisher from the connected form.
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
///     |connected| connected.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if construction, connection, subscription, delivery, ack,
/// shutdown, or the aliased-handle behaviour does not follow the contract.
pub async fn lifecycle<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const SUBJECT: &str = "conformance.lifecycle";

    let connected = make_broker()
        .connect()
        .await
        .expect("broker must connect after synchronous construction");

    let mut subscriber = make_source(SUBJECT)
        .subscribe(&connected)
        .await
        .expect("subscription source must open against the connected form");
    let publisher = make_publisher(&connected);

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

    let _closed = connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");

    // The ladder makes owner-side misuse unrepresentable; the aliased publisher created before
    // the shutdown is the surface that must stay honest at runtime.
    assert!(
        publisher
            .publish(OutgoingMessage::new(SUBJECT, b"post-shutdown".as_slice()))
            .await
            .is_err(),
        "publish through a handle aliasing the closed connection must error",
    );
}

async fn ordering<C: TestableBroker + Subscribe>(broker: C) {
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
    broker.shutdown().await.expect("shutdown failed");
}

async fn publish_after_subscribe<C: TestableBroker + Subscribe>(broker: C) {
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
    broker.shutdown().await.expect("shutdown failed");
}

async fn ack_consumes_delivery<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.ack")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.ack", b"one".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "ack_consumes_delivery").await;
    msg.ack().await.expect("ack failed");

    expect_no_more(&mut stream, "ack_consumes_delivery").await;
    broker.shutdown().await.expect("shutdown failed");
}

async fn nack_with_requeue_redelivers<C: TestableBroker + Subscribe>(broker: C) {
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
    broker.shutdown().await.expect("shutdown failed");
}

async fn nack_without_requeue_drops<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.drop")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.drop", b"gone".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "nack_without_requeue").await;
    msg.nack(false).await.expect("nack failed");

    expect_no_more(&mut stream, "nack_without_requeue").await;
    broker.shutdown().await.expect("shutdown failed");
}

async fn headers_propagate<C: TestableBroker + Subscribe>(broker: C) {
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
    broker.shutdown().await.expect("shutdown failed");
}

async fn published_log_observes_publishes<C: TestableBroker + Subscribe>(broker: C) {
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
    broker.shutdown().await.expect("shutdown failed");
}

pub(crate) async fn expect_next<S, M, E>(stream: &mut S, label: &str) -> M
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let item = timeout(DEFAULT_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: stream timed out"));
    let item = item.unwrap_or_else(|| panic!("{label}: stream ended unexpectedly"));
    item.unwrap_or_else(|err| panic!("{label}: stream yielded error: {err:?}"))
}

pub(crate) async fn expect_no_more<S, M, E>(stream: &mut S, label: &str)
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let result = timeout(NEGATIVE_WAIT, stream.next()).await;
    assert!(
        result.is_err(),
        "{label}: expected no further deliveries within {NEGATIVE_WAIT:?}",
    );
}
