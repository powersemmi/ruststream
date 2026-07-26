//! Integration tests for the manual transaction scope: publishes issued through a
//! [`TransactionScope`] become visible together on commit, never after an abort, and the
//! wrapper is reusable once a scope settles.
//!
//! [`TransactionScope`]: ruststream::runtime::TransactionScope
#![cfg(all(feature = "memory", feature = "json"))]

use std::pin::pin;

use futures::{FutureExt, StreamExt};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::TypedPublisher;
use ruststream::{IncomingMessage, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_makes_scope_publishes_visible_atomically() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders.settled");
    let publisher = TypedPublisher::new(broker.publisher()).transactional();

    let mut scope = publisher.begin().await.expect("begin failed");
    scope
        .publish("orders.settled", &Order { id: 1 })
        .await
        .expect("publish failed");
    scope
        .publish("orders.settled", &Order { id: 2 })
        .await
        .expect("publish failed");

    // The memory broker fans out synchronously, so an uncommitted publish that leaked would
    // already be in the queue here.
    let mut stream = pin!(subscriber.stream());
    assert!(
        stream.next().now_or_never().flatten().is_none(),
        "scope publishes became visible before commit"
    );

    scope.commit().await.expect("commit failed");

    for expected in [1, 2] {
        let msg = stream
            .next()
            .now_or_never()
            .flatten()
            .expect("committed publish did not arrive")
            .expect("memory subscriber never errors");
        let order: Order = serde_json::from_slice(msg.payload()).expect("decode failed");
        assert_eq!(order.id, expected);
        msg.ack().await.expect("ack failed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_discards_scope_publishes_and_frees_the_wrapper() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders.settled");
    let publisher = TypedPublisher::new(broker.publisher()).transactional();

    let mut scope = publisher.begin().await.expect("begin failed");
    scope
        .publish("orders.settled", &Order { id: 1 })
        .await
        .expect("publish failed");
    scope.abort().await.expect("abort failed");

    let mut stream = pin!(subscriber.stream());
    assert!(
        stream.next().now_or_never().flatten().is_none(),
        "aborted publish became visible"
    );

    // The wrapper is free again: a fresh scope on the same handle commits normally.
    let mut scope = publisher.begin().await.expect("second begin failed");
    scope
        .publish("orders.settled", &Order { id: 3 })
        .await
        .expect("publish failed");
    scope.commit().await.expect("commit failed");

    let msg = stream
        .next()
        .now_or_never()
        .flatten()
        .expect("committed publish did not arrive")
        .expect("memory subscriber never errors");
    let order: Order = serde_json::from_slice(msg.payload()).expect("decode failed");
    assert_eq!(order.id, 3);
    msg.ack().await.expect("ack failed");
}
