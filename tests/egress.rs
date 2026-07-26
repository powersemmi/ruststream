//! Egress injection: a handler receives a live publisher as a parameter, paired by the runtime
//! from the source attached at the include site.
#![cfg(all(feature = "memory", feature = "macros", feature = "json"))]

use std::time::Duration;

use futures::StreamExt;
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{AppInfo, Egress, HandlerResult, RustStream};
use ruststream::{IncomingMessage, OutgoingMessage, Publisher, Subscriber, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    id: u64,
}

/// The destination is computed per message: exactly the case reply publishing cannot cover and
/// the injected publisher exists for.
#[subscriber("egress.in")]
async fn forward(event: &Event, Egress(out): Egress<MemoryPublisher>) -> HandlerResult {
    let dest = if event.id % 2 == 0 {
        "egress.even"
    } else {
        "egress.odd"
    };
    let payload = serde_json::to_vec(event).expect("serializable");
    if out
        .publish(OutgoingMessage::new(dest, payload.as_slice()))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

async fn expect_id(sub: &mut ruststream::memory::MemorySubscriber, id: u64) {
    let mut stream = std::pin::pin!(sub.stream());
    let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("delivery timed out")
        .expect("stream ended")
        .expect("stream errored");
    let event: Event = serde_json::from_slice(msg.payload()).expect("decodes");
    assert_eq!(event.id, id);
    msg.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_publisher_reaches_the_handler_live() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    let mut even = broker.subscribe("egress.even");
    let mut odd = broker.subscribe("egress.odd");

    let app = RustStream::new(AppInfo::new("egress", "0.1.0")).with_broker(broker, |b| {
        b.include(forward).publisher(MemoryPublish);
    });
    let running = app.start().await.expect("startup failed");

    for id in [2u64, 3u64] {
        ingress
            .publish(OutgoingMessage::new(
                "egress.in",
                serde_json::to_vec(&Event { id }).unwrap().as_slice(),
            ))
            .await
            .expect("publish");
    }
    expect_id(&mut even, 2).await;
    expect_id(&mut odd, 3).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("egress.crossing")]
async fn crossing(event: &Event, Egress(out): Egress<MemoryPublisher>) -> HandlerResult {
    let payload = serde_json::to_vec(event).expect("serializable");
    if out
        .publish(OutgoingMessage::new("egress.other", payload.as_slice()))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

/// The cross-broker case: the handler consumes one broker and its injected publisher targets
/// another, through a token minted by the target broker's scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_token_injects_a_foreign_brokers_publisher() {
    let ingress_broker = MemoryBroker::new();
    let other = MemoryBroker::new();
    let ingress = ingress_broker.publisher();
    let mut sink = other.subscribe("egress.other");

    let mut token = None;
    let app = RustStream::new(AppInfo::new("egress-cross", "0.1.0"))
        .with_broker(other, |b| {
            token = Some(b.bind(MemoryPublish));
        })
        .with_broker(ingress_broker, |b| {
            b.include(crossing)
                .publisher(token.take().expect("token bound"));
        });
    let running = app.start().await.expect("startup failed");

    ingress
        .publish(OutgoingMessage::new(
            "egress.crossing",
            serde_json::to_vec(&Event { id: 9 }).unwrap().as_slice(),
        ))
        .await
        .expect("publish");
    expect_id(&mut sink, 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
