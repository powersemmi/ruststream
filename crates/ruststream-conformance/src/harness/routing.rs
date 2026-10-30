//! The routing contract's scenarios: each takes a connected form and drives it through its own
//! subscribe and inject surface.

use bytes::Bytes;
use ruststream_core::{
    AckError, HeaderMap, IncomingMessage, OutgoingMessage, Subscribe, Subscriber,
};
use ruststream_runtime::testing::{Backlog, TestableBroker};

use super::{expect_next, expect_no_more};

pub(super) async fn ordering<C: TestableBroker + Subscribe>(broker: C) {
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
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    broker.shutdown().await.expect("shutdown failed");
}

/// A message published before the subscription opened reaches it, or does not, exactly as the
/// broker declares through [`TestableBroker::backlog`]; the one published after reaches it either
/// way.
pub(super) async fn publish_before_subscribe<C: TestableBroker + Subscribe>(broker: C) {
    let backlog = broker.backlog();
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
    if backlog == Backlog::Delivered {
        let early = expect_next(&mut stream, "publish_before_subscribe").await;
        assert_eq!(
            early.payload(),
            b"before-subscribe",
            "the broker declares `Backlog::Delivered`: a subscription must first receive what was \
             published to its name before it opened",
        );
        settle_ack(early).await;
    }
    let msg = expect_next(&mut stream, "publish_before_subscribe").await;
    assert_eq!(
        msg.payload(),
        b"after-subscribe",
        "the broker declares `Backlog::{backlog:?}`: {}",
        match backlog {
            Backlog::Missed =>
                "a subscription must receive only messages published after it opened",
            Backlog::Delivered => "the message published after the subscription opened comes next",
            other => unknown_backlog(other),
        },
    );
    settle_ack(msg).await;
    broker.shutdown().await.expect("shutdown failed");
}

/// A [`Backlog`] answer added after this suite was written: the suite has no scenario for it.
pub(crate) fn unknown_backlog(backlog: Backlog) -> ! {
    panic!(
        "the broker declares `Backlog::{backlog:?}`, which this conformance suite has no scenario for"
    )
}

/// Acknowledges `msg`, accepting a transport that cannot acknowledge.
async fn settle_ack<M: IncomingMessage>(msg: M) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
}

pub(super) async fn ack_consumes_delivery<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.ack")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.ack", b"one".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "ack_consumes_delivery").await;
    // A transport with no acknowledgement consumes the delivery by delivering it, so the
    // assertion below - one publish, one delivery - is the same contract either way.
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    expect_no_more(&mut stream, "ack_consumes_delivery").await;
    broker.shutdown().await.expect("shutdown failed");
}

pub(super) async fn nack_with_requeue_redelivers<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.requeue")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new(
        "conformance.requeue",
        b"retry-me".as_slice(),
    ));

    let mut stream = std::pin::pin!(subscriber.stream());
    let first = expect_next(&mut stream, "nack_with_requeue first").await;
    assert_eq!(
        first.payload(),
        b"retry-me",
        "the subscriber must receive the published message"
    );
    // The only scenario whose assertion IS the settlement: a transport that reports the requeue
    // unsupported has no redelivery to observe, so the scenario ends instead of accepting any
    // answer - reading the redelivery of a message the transport never took back would pass a
    // broker whose retries silently lose messages.
    let requeued = match first.nack(true).await {
        Ok(()) => true,
        Err(AckError::Unsupported) => false,
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    };

    if requeued {
        let second = expect_next(&mut stream, "nack_with_requeue second").await;
        assert_eq!(
            second.payload(),
            b"retry-me",
            "nack(requeue=true) must redeliver the same payload",
        );
        match second.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    broker.shutdown().await.expect("shutdown failed");
}

pub(super) async fn nack_without_requeue_drops<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.drop")
        .await
        .expect("subscribe failed");

    broker.inject(OutgoingMessage::new("conformance.drop", b"gone".as_slice()));

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "nack_without_requeue").await;
    // Dropping is what a transport with no settlement does with every delivery anyway, so the
    // assertion below holds for both answers and stays checked for both.
    match msg.nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    }

    expect_no_more(&mut stream, "nack_without_requeue").await;
    broker.shutdown().await.expect("shutdown failed");
}

pub(super) async fn headers_propagate<C: TestableBroker + Subscribe>(broker: C) {
    let mut subscriber = Subscribe::subscribe(&broker, "conformance.headers")
        .await
        .expect("subscribe failed");

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json");
    headers.insert("X-Tenant", Bytes::from_static(b"acme"));

    broker.inject(
        OutgoingMessage::new("conformance.headers", b"{}".as_slice()).with_headers(headers),
    );

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = expect_next(&mut stream, "headers_propagate").await;
    assert_eq!(
        msg.headers().content_type(),
        Some("application/json"),
        "the content type must reach the subscriber"
    );
    assert_eq!(
        msg.headers().get("x-tenant"),
        Some(b"acme".as_slice()),
        "a custom header must reach the subscriber"
    );
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    broker.shutdown().await.expect("shutdown failed");
}

pub(super) async fn published_log_observes_publishes<C: TestableBroker + Subscribe>(broker: C) {
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
    assert_eq!(
        observed[0].payload(),
        b"first",
        "the publish log must keep publish order"
    );
    assert_eq!(
        observed[1].payload(),
        b"second",
        "the publish log must keep publish order"
    );
    broker.shutdown().await.expect("shutdown failed");
}
