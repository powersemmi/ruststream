//! Integration tests for the router and middleware stack, using `MemoryBroker`.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    DecodeFailure, HandlerExt, HandlerMetadata, HandlerResult, Router, layers,
};
use ruststream::{Broker, OutgoingMessage, Publisher};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Order {
    id: u32,
    total: f64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_dispatches_typed_messages() {
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();

    let received = Arc::new(AtomicU32::new(0));
    let received_clone = Arc::clone(&received);

    let handler = ruststream::runtime::typed(JsonCodec, move |order: Order| {
        let received = Arc::clone(&received_clone);
        async move {
            assert!(order.total > 0.0);
            received.fetch_add(order.id, Ordering::SeqCst);
            HandlerResult::Ack
        }
    });

    let mut router = Router::new();
    router.handle(
        subscriber,
        handler,
        HandlerMetadata::typed::<Order>("orders"),
    );

    let payload = serde_json::to_vec(&Order { id: 7, total: 9.99 }).unwrap();
    publisher
        .publish(OutgoingMessage::new("orders", &payload))
        .await
        .unwrap();
    publisher
        .publish(OutgoingMessage::new(
            "orders",
            &serde_json::to_vec(&Order { id: 3, total: 1.0 }).unwrap(),
        ))
        .await
        .unwrap();

    wait_for(
        || received.load(Ordering::SeqCst) == 10,
        Duration::from_secs(1),
    )
    .await;
    router.shutdown();
    router.run().await.unwrap();
    broker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_records_handler_metadata() {
    let broker = MemoryBroker::new();
    let mut router = Router::new();
    router.handle(
        broker.subscribe("orders"),
        |_msg: &_| async { HandlerResult::Ack },
        HandlerMetadata::typed::<Order>("orders").with_description("processes incoming orders"),
    );
    router.handle(
        broker.subscribe("alerts"),
        |_msg: &_| async { HandlerResult::Ack },
        HandlerMetadata::raw("alerts"),
    );

    assert_eq!(router.handlers().len(), 2);
    assert_eq!(router.handlers()[0].topic, "orders");
    assert_eq!(
        router.handlers()[0].description.as_deref(),
        Some("processes incoming orders"),
    );
    assert_eq!(router.handlers()[1].input_type, "bytes");

    router.shutdown();
    router.run().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_handler_drops_undecodable_payload() {
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();

    let calls = Arc::new(AtomicU32::new(0));
    let calls_clone = Arc::clone(&calls);

    let handler = ruststream::runtime::typed(JsonCodec, move |_: Order| {
        let calls = Arc::clone(&calls_clone);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            HandlerResult::Ack
        }
    })
    .on_decode_failure(DecodeFailure::Drop);

    let mut router = Router::new();
    router.handle(
        subscriber,
        handler,
        HandlerMetadata::typed::<Order>("orders"),
    );

    publisher
        .publish(OutgoingMessage::new("orders", b"not json"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "handler must not run on decode failure"
    );
    router.shutdown();
    router.run().await.unwrap();
    broker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracing_layer_passes_messages_through() {
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe("events");
    let publisher = broker.publisher();

    let seen = Arc::new(AtomicU32::new(0));
    let seen_clone = Arc::clone(&seen);

    let base = move |_msg: &_| {
        let seen = Arc::clone(&seen_clone);
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            HandlerResult::Ack
        }
    };
    let handler = base.with(layers::TracingLayer::default());

    let mut router = Router::new();
    router.handle(subscriber, handler, HandlerMetadata::raw("events"));

    publisher
        .publish(OutgoingMessage::new("events", b"hello"))
        .await
        .unwrap();

    wait_for(|| seen.load(Ordering::SeqCst) == 1, Duration::from_secs(1)).await;
    router.shutdown();
    router.run().await.unwrap();
    broker.shutdown().await.unwrap();
}

async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "condition not met within {timeout:?}");
}
