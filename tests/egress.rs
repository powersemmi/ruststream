//! Egress injection: a handler receives a live publisher as a parameter, paired by the runtime
//! from the source attached at the include site.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use std::time::Duration;

use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{AppInfo, Egress, HandlerResult, RustStream};
use ruststream::testing::expect_published;
use ruststream::{Broker, OutgoingMessage, Publisher, subscriber};
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

async fn expect_id(observer: &ConnectedMemoryBroker, name: &str, id: u64) {
    let seen = expect_published(observer, name, 1, Duration::from_secs(2)).await;
    assert_eq!(seen.len(), 1, "expected one publish on {name}");
    let event: Event = serde_json::from_slice(seen[0].payload()).expect("decodes");
    assert_eq!(event.id, id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_injected_publisher_reaches_the_handler_live() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();
    // The observing side needs the TestableBroker surface, which lives on the connected form.
    let observer = Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");

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
    expect_id(&observer, "egress.even", 2).await;
    expect_id(&observer, "egress.odd", 3).await;

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
    let observer = Broker::connect(other.clone())
        .await
        .expect("memory connect is infallible");

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
    expect_id(&observer, "egress.other", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
