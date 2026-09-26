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

use ruststream::memory::prelude::*;
use ruststream::testing::{TestApp, expect_published};

use common::{Event, Wire, connected};

#[subscriber("pairing.seeded")]
async fn consume(_event: &Event) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The first publish: the scope-level `after_startup` runs post-connect and post-subscribe with
/// an already-paired publisher, so it feeds the app's own subscription (a pre-subscribe publish
/// would reach nobody on the in-memory bus) and nothing leaves the wiring closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_scope_hook_publishes_first_with_a_paired_publisher() {
    let app =
        RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(consume);
            b.after_startup(Publish, async move |publisher| {
                publisher
                    .message(&Event { id: 1 })
                    .to("pairing.seeded")
                    .publish()
                    .await
            });
        });

    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("pairing.seeded")
        .assert_called_once()
        .with(&Event { id: 1 })
        .settled(HandlerOutcome::ack());
}

/// A sibling task's publisher: paired through the running handle, whose existence witnesses
/// that startup connected the broker. The running handle is the subject, and the harness hands
/// out none, so the app is started by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_running_handle_pairs_a_token_for_sibling_tasks() {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8))).bindable();
    let observer = connected(broker.broker()).await;
    let egress = broker.bind(Publish);

    let app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |b| {
        b.include(consume);
    });
    let running = app.start().await.expect("startup failed");

    let publisher = running
        .publisher(egress)
        .await
        .expect("pairing after start is infallible for memory");
    publisher
        .message(&Wire::of(b"late"))
        .to("pairing.sibling")
        .publish()
        .await
        .expect("publish");
    let seen = expect_published(&observer, "pairing.sibling", 1, Duration::from_secs(2)).await;
    assert_eq!(seen[0].payload(), b"late");

    running.shutdown().await.expect("graceful shutdown failed");
}

/// Pairing before startup is the one representable misuse left on this path, and it reports a
/// clear error instead of hanging or panicking.
#[tokio::test]
async fn pairing_before_startup_reports_a_clear_error() {
    let broker = MemoryBroker::new().bindable();
    let token = broker.bind(Publish);
    let _app = RustStream::new(AppInfo::new("pairing", "0.1.0")).with_broker(broker, |_b| {});

    let err = token
        .live()
        .await
        .expect_err("pairing before startup must fail");
    assert!(err.to_string().contains("not connected"), "{err}");
}
