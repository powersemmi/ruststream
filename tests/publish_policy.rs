//! The publish-policy seam: a policy is pure declaration, pairing against a connected broker is
//! the only way to a live publisher, and pairing is functorial over the typed combinator stack.
#![cfg(all(feature = "memory", feature = "macros", feature = "json"))]

use std::time::Duration;

use futures::StreamExt;
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryRequest};
use ruststream::runtime::{
    AppInfo, Outgoing, PublishContext, PublishTransform, RustStream, TypedPublisher,
};
use ruststream::{
    Broker, IncomingMessage, OutgoingMessage, PublishPolicy, Publisher, Subscriber, subscriber,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Receipt {
    id: u32,
}

/// Stamps every outgoing reply, so the test can prove the transform stack survived pairing.
struct Envelope;

impl<C> PublishTransform<C> for Envelope {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}

#[tokio::test]
async fn a_bare_policy_pairs_into_a_live_publisher() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("policy.out");

    let connected = broker
        .connect()
        .await
        .expect("memory connect is infallible");
    let publisher = MemoryPublish
        .pair(&connected)
        .await
        .expect("memory pairing is infallible");

    publisher
        .publish(OutgoingMessage::new("policy.out", b"paired".as_slice()))
        .await
        .expect("publish through the paired publisher");

    let mut stream = std::pin::pin!(sub.stream());
    let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("delivery timed out")
        .expect("stream ended")
        .expect("stream errored");
    assert_eq!(msg.payload(), b"paired");
    msg.ack().await.expect("ack");
}

#[tokio::test]
async fn a_request_policy_pairs_into_a_requester() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("memory connect is infallible");
    let requester = MemoryRequest
        .pair(&connected)
        .await
        .expect("memory pairing is infallible");
    // No responder is subscribed; the requester must fail fast on timeout, proving it is live
    // and bound to this broker's bus.
    let unanswered = ruststream::RequestReply::request(
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

/// Pairing a typed stack keeps the codec and the transform: the paired form is the same wiring
/// type over the live leaf, accepted by the reply mounts as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_typed_policy_stack_pairs_functorially() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let mut replies_sub = broker.subscribe("policy.replies");

    // The stack itself is a policy: pairing it manually against a connected clone yields the
    // same wiring type over the live leaf (the functorial half of the seam)...
    let connected = Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");
    let paired = TypedPublisher::new(MemoryPublish)
        .transform(Envelope)
        .pair(&connected)
        .await
        .expect("memory pairing is infallible");
    let _type_check: TypedPublisher<ruststream::memory::MemoryPublisher, _, _> = paired;

    // ...while the registration takes the unpaired stack and the runtime pairs it at startup.
    let replies = TypedPublisher::new(MemoryPublish).transform(Envelope);
    let app = RustStream::new(AppInfo::new("policy", "0.1.0")).with_broker(broker, |b| {
        b.include(respond).publisher(replies);
    });
    let running = app.start().await.expect("startup failed");

    publisher
        .publish(OutgoingMessage::new(
            "policy.requests",
            serde_json::to_vec(&Order { id: 7 }).unwrap().as_slice(),
        ))
        .await
        .expect("publish request");

    let mut stream = std::pin::pin!(replies_sub.stream());
    let reply = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("reply timed out")
        .expect("stream ended")
        .expect("stream errored");
    assert_eq!(
        reply.headers().get("x-envelope"),
        Some(b"1".as_slice()),
        "the transform stack must survive pairing",
    );
    reply.ack().await.expect("ack");

    running.shutdown().await.expect("graceful shutdown failed");
}
