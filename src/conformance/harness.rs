//! Conformance test suite that any [`TestClient`] implementation must pass.
//!
//! Broker authors prove their implementation honours the [`Broker`] contract by running the
//! suite against the [`TestClient`] their crate ships under the `testing` feature. Each test
//! starts from a fresh broker instance produced by the caller-supplied factory.
//!
//! # Examples
//!
//! The example uses [`crate::memory::MemoryBroker`] as a stand-in broker, so it needs the
//! `memory` feature; a broker crate substitutes its own `TestClient` here.
//!
//! ```no_run
//! # #[cfg(feature = "memory")]
//! # async fn run() {
//! use ruststream::{conformance::harness, memory::MemoryBroker};
//!
//! harness::run_suite(|| async { Ok::<_, std::convert::Infallible>(MemoryBroker::new()) }).await;
//! # }
//! ```

use std::{future::Future, time::Duration};

use crate::{
    Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber, testing::TestClient,
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
pub async fn run_suite<T, F, Fut, E>(factory: F)
where
    T: TestClient<Error = E>,
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<T, E>> + Send,
    E: std::fmt::Debug,
{
    let fresh = || async { factory().await.expect("test client factory failed") };

    ordering(fresh().await).await;
    publish_after_subscribe(fresh().await).await;
    ack_consumes_delivery(fresh().await).await;
    nack_with_requeue_redelivers(fresh().await).await;
    nack_without_requeue_drops(fresh().await).await;
    headers_propagate(fresh().await).await;
    expect_published_observes_publishes(fresh().await).await;
}

async fn ordering<T: TestClient>(client: T) {
    let mut subscriber = client
        .subscribe("conformance.ordering")
        .await
        .expect("subscribe failed");
    let publisher = client.publisher().await.expect("publisher failed");

    for i in 0..10u32 {
        publisher
            .publish(OutgoingMessage::new(
                "conformance.ordering",
                i.to_be_bytes().as_slice(),
            ))
            .await
            .expect("publish failed");
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
    client.shutdown().await.expect("shutdown failed");
}

async fn publish_after_subscribe<T: TestClient>(client: T) {
    let publisher = client.publisher().await.expect("publisher failed");

    publisher
        .publish(OutgoingMessage::new(
            "conformance.late",
            b"before-subscribe".as_slice(),
        ))
        .await
        .expect("publish failed");

    let mut subscriber = client
        .subscribe("conformance.late")
        .await
        .expect("subscribe failed");

    publisher
        .publish(OutgoingMessage::new(
            "conformance.late",
            b"after-subscribe".as_slice(),
        ))
        .await
        .expect("publish failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "publish_after_subscribe").await;
    assert_eq!(
        msg.payload(),
        b"after-subscribe",
        "subscriber must receive only messages published after subscription opened",
    );
    msg.ack().await.expect("ack failed");
    client.shutdown().await.expect("shutdown failed");
}

async fn ack_consumes_delivery<T: TestClient>(client: T) {
    let mut subscriber = client
        .subscribe("conformance.ack")
        .await
        .expect("subscribe failed");
    let publisher = client.publisher().await.expect("publisher failed");

    publisher
        .publish(OutgoingMessage::new("conformance.ack", b"one".as_slice()))
        .await
        .expect("publish failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "ack_consumes_delivery").await;
    msg.ack().await.expect("ack failed");

    expect_no_more(&mut stream, "ack_consumes_delivery").await;
    client.shutdown().await.expect("shutdown failed");
}

async fn nack_with_requeue_redelivers<T: TestClient>(client: T) {
    let mut subscriber = client
        .subscribe("conformance.requeue")
        .await
        .expect("subscribe failed");
    let publisher = client.publisher().await.expect("publisher failed");

    publisher
        .publish(OutgoingMessage::new(
            "conformance.requeue",
            b"retry-me".as_slice(),
        ))
        .await
        .expect("publish failed");

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
    client.shutdown().await.expect("shutdown failed");
}

async fn nack_without_requeue_drops<T: TestClient>(client: T) {
    let mut subscriber = client
        .subscribe("conformance.drop")
        .await
        .expect("subscribe failed");
    let publisher = client.publisher().await.expect("publisher failed");

    publisher
        .publish(OutgoingMessage::new("conformance.drop", b"gone".as_slice()))
        .await
        .expect("publish failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "nack_without_requeue").await;
    msg.nack(false).await.expect("nack failed");

    expect_no_more(&mut stream, "nack_without_requeue").await;
    client.shutdown().await.expect("shutdown failed");
}

async fn headers_propagate<T: TestClient>(client: T) {
    let mut subscriber = client
        .subscribe("conformance.headers")
        .await
        .expect("subscribe failed");
    let publisher = client.publisher().await.expect("publisher failed");

    let mut headers = Headers::new();
    headers.insert("Content-Type", "application/json");
    headers.insert("X-Tenant", Bytes::from_static(b"acme"));

    publisher
        .publish(
            OutgoingMessage::new("conformance.headers", b"{}".as_slice()).with_headers(headers),
        )
        .await
        .expect("publish failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "headers_propagate").await;
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().get("x-tenant"), Some(b"acme".as_slice()));
    msg.ack().await.expect("ack failed");
    client.shutdown().await.expect("shutdown failed");
}

async fn expect_published_observes_publishes<T: TestClient>(client: T) {
    let publisher = client.publisher().await.expect("publisher failed");
    publisher
        .publish(OutgoingMessage::new(
            "conformance.observe",
            b"first".as_slice(),
        ))
        .await
        .expect("publish failed");
    publisher
        .publish(OutgoingMessage::new(
            "conformance.observe",
            b"second".as_slice(),
        ))
        .await
        .expect("publish failed");

    let observed = client
        .expect_published("conformance.observe", 2, DEFAULT_TIMEOUT)
        .await
        .expect("expect_published failed");
    assert_eq!(
        observed.len(),
        2,
        "expect_published must observe every publish"
    );
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
    client.shutdown().await.expect("shutdown failed");
}

async fn expect_next<S, M, E>(stream: &mut S, label: &str) -> M
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

async fn expect_no_more<S, M, E>(stream: &mut S, label: &str)
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
