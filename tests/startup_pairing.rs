//! Startup and post-start pairing of bound tokens: the first publish from `after_startup`, and
//! a sibling task's publisher obtained through the running handle.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::time::Duration;

use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerResult, PublishExt, RustStream};
use ruststream::testing::expect_published;
use ruststream::{Broker, subscriber};
use serde::{Deserialize, Serialize};

use common::connected;

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    id: u64,
}

#[subscriber("pairing.seeded")]
async fn consume(_event: &Event) -> HandlerResult {
    HandlerResult::Ack
}

async fn expect_payload(observer: &ConnectedMemoryBroker, name: &str, expected: &[u8]) {
    let seen = expect_published(observer, name, 1, Duration::from_secs(2)).await;
    assert_eq!(seen.len(), 1, "expected one publish on {name}");
    assert_eq!(seen[0].payload(), expected);
}

/// The first publish: the scope-level `after_startup` runs post-connect and post-subscribe with
/// an already-paired publisher, so it feeds the app's own subscription (a pre-subscribe publish
/// would reach nobody on the in-memory bus) and nothing leaves the wiring closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_scope_hook_publishes_first_with_a_paired_publisher() {
    let broker = MemoryBroker::new();
    let observer = connected(&broker).await;

    let app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |b| {
        b.include(consume);
        b.after_startup(MemoryPublish, async move |publisher| {
            publisher.raw(b"first").to("pairing.seeded").publish().await
        });
    });

    let running = app.start().await.expect("startup failed");
    expect_payload(&observer, "pairing.seeded", b"first").await;
    running.shutdown().await.expect("graceful shutdown failed");
}

/// A sibling task's publisher: paired through the running handle, whose existence witnesses
/// that startup connected the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_running_handle_pairs_a_token_for_sibling_tasks() {
    let broker = MemoryBroker::new().bindable();
    let observer = connected(broker.broker()).await;
    let egress = broker.bind(MemoryPublish);

    let app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |b| {
        b.include(consume);
    });
    let running = app.start().await.expect("startup failed");

    let publisher = running
        .publisher(egress)
        .await
        .expect("pairing after start is infallible for memory");
    publisher
        .raw(b"late")
        .to("pairing.sibling")
        .publish()
        .await
        .expect("publish");
    expect_payload(&observer, "pairing.sibling", b"late").await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// Pairing before startup is the one representable misuse left on this path, and it reports a
/// clear error instead of hanging or panicking.
#[tokio::test]
async fn pairing_before_startup_reports_a_clear_error() {
    let broker = MemoryBroker::new().bindable();
    let token = broker.bind(MemoryPublish);
    let _app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |_b| {});

    let err = token
        .live()
        .await
        .expect_err("pairing before startup must fail");
    assert!(err.to_string().contains("not connected"), "{err}");
}
