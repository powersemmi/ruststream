//! Startup and post-start pairing of bound tokens: the first publish from `after_startup`, and
//! a sibling task's publisher obtained through the running handle.
#![cfg(all(feature = "memory", feature = "macros", feature = "json"))]

use std::time::Duration;

use futures::StreamExt;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::{IncomingMessage, OutgoingMessage, PairError, Publisher, Subscriber, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    id: u64,
}

#[subscriber("pairing.seeded")]
async fn consume(_event: &Event) -> HandlerResult {
    HandlerResult::Ack
}

async fn expect_payload(sub: &mut ruststream::memory::MemorySubscriber, expected: &[u8]) {
    let mut stream = std::pin::pin!(sub.stream());
    let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("delivery timed out")
        .expect("stream ended")
        .expect("stream errored");
    assert_eq!(msg.payload(), expected);
    msg.ack().await.expect("ack");
}

/// The first publish: `after_startup` runs post-connect and post-subscribe, so a token paired
/// there feeds the app's own subscription (a pre-subscribe publish would reach nobody on the
/// in-memory bus).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_pairs_in_after_startup_for_the_first_publish() {
    let broker = MemoryBroker::new();
    let mut observer = broker.subscribe("pairing.seeded");

    let mut seed = None;
    let app = RustStream::new(AppInfo::new("pairing", "0.1.0"))
        .with_broker(broker, |b| {
            seed = Some(b.bind(MemoryPublish));
            b.include(consume);
        })
        .after_startup(async move |_state| {
            let publisher = seed.take().expect("token bound").live().await?;
            publisher
                .publish(OutgoingMessage::new("pairing.seeded", b"first".as_slice()))
                .await
                .map_err(PairError::new)
        });

    let running = app.start().await.expect("startup failed");
    expect_payload(&mut observer, b"first").await;
    running.shutdown().await.expect("graceful shutdown failed");
}

/// A sibling task's publisher: paired through the running handle, whose existence witnesses
/// that startup connected the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_running_handle_pairs_a_token_for_sibling_tasks() {
    let broker = MemoryBroker::new();
    let mut observer = broker.subscribe("pairing.sibling");

    let mut egress = None;
    let app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |b| {
        egress = Some(b.bind(MemoryPublish));
        b.include(consume);
    });
    let running = app.start().await.expect("startup failed");

    let publisher = running
        .publisher(egress.take().expect("token bound"))
        .await
        .expect("pairing after start is infallible for memory");
    publisher
        .publish(OutgoingMessage::new("pairing.sibling", b"late".as_slice()))
        .await
        .expect("publish");
    expect_payload(&mut observer, b"late").await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// Pairing before startup is the one representable misuse left on this path, and it reports a
/// clear error instead of hanging or panicking.
#[tokio::test]
async fn pairing_before_startup_reports_a_clear_error() {
    let broker = MemoryBroker::new();
    let mut token = None;
    let _app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |b| {
        token = Some(b.bind(MemoryPublish));
    });

    let err = token
        .take()
        .expect("token bound")
        .live()
        .await
        .expect_err("pairing before startup must fail");
    assert!(err.to_string().contains("not connected"), "{err}");
}
