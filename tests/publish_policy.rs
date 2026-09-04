//! The publish-policy seam: a policy is pure declaration, pairing against a connected broker is
//! the only way to a live publisher, and pairing is functorial over the typed combinator stack.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext, PublishTransform};
use ruststream::testing::expect_published;

use common::{Order, Receipt, Wire, connected};

/// Stamps every outgoing reply, so the test can prove the transform stack survived pairing.
struct Envelope;

impl<C> PublishTransform<C> for Envelope {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}

#[tokio::test]
async fn a_bare_policy_pairs_into_a_live_publisher() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("memory connect is infallible");
    let publisher = Publish
        .pair(&connected)
        .await
        .expect("memory pairing is infallible");

    publisher
        .message(&Wire::of(b"paired"))
        .to("policy.out")
        .publish()
        .await
        .expect("publish through the paired publisher");

    let seen = expect_published(&connected, "policy.out", 1, Duration::from_secs(2)).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].payload(), b"paired");
}

#[tokio::test]
async fn a_request_policy_pairs_into_a_requester() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("memory connect is infallible");
    let requester = Request
        .pair(&connected)
        .await
        .expect("memory pairing is infallible");
    // No responder is subscribed; the requester must fail fast on timeout, proving it is live
    // and bound to this broker's bus.
    let unanswered = RequestReply::request(
        &requester,
        OutgoingMessage::new("policy.void", b"ping".as_slice()),
        Duration::from_millis(50),
    )
    .await;
    assert!(unanswered.is_err(), "{unanswered:?}");
}

#[subscriber("policy.requests", publish("policy.replies"))]
async fn respond(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The reply wiring the chain builds keeps the codec and the transform through the pairing: the
/// live leaf publishes with what the mount site named.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_wiring_keeps_its_transform_through_the_pairing() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let live = connected(&broker).await;

    // The registration carries the wiring the chain built, and the runtime pairs it at startup.
    let app = RustStream::new(AppInfo::new("policy", "0.1.0")).with_broker(broker, |b| {
        b.include(respond).out(Reply, Publish).transform(Envelope);
    });
    let running = app.start().await.expect("startup failed");

    publisher
        .message(&Order { id: 7 })
        .to("policy.requests")
        .publish()
        .await
        .expect("publish request");

    let seen = expect_published(&live, "policy.replies", 1, Duration::from_secs(2)).await;
    assert_eq!(seen.len(), 1, "the reply must be published");
    assert_eq!(
        seen[0].headers().get("x-envelope"),
        Some(b"1".as_slice()),
        "the transform stack must survive pairing",
    );

    running.shutdown().await.expect("graceful shutdown failed");
}
