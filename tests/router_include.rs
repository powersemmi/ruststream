//! Integration tests for the `Router` include family (subscribe and batch forms), in both codec
//! forms: the default codec and a chain codec set with `with_codec`. Also covers `merge`, the
//! router's own `layer` stack, and `handlers()` metadata collection.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::Order;
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::runtime::{AppInfo, HandlerOutcome, Router, RustStream, layers::TracingLayer};
use ruststream::subscriber;
use ruststream::testing::TestApp;

/// Publishes one order to each topic and asserts every one of them reached its handler exactly
/// once. Nothing else is recorded: the subject is which registrations mount, not what they do.
async fn drive_all<S: Send + Sync + 'static>(tb: &TestApp<S>, topics: &[&str]) {
    for topic in topics {
        tb.message(&Order { id: 1 })
            .to(*topic)
            .publish()
            .await
            .expect("publish");
    }
    for topic in topics {
        tb.broker::<MemoryBroker>()
            .subscriber(topic)
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
    }
}

#[subscriber("ri-plain")]
async fn ri_plain(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

// A broker source expression rather than a bare name: the definition is where a subscription
// source belongs, and the attribute takes the broker's own source builder.
#[subscriber(MemorySource::new("ri-on"))]
async fn ri_on(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("ri-batch")]
async fn ri_batch(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[subscriber(MemorySource::new("ri-batch-on"))]
async fn ri_batch_on(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

/// The default-codec router forms dispatch, whether the definition names its source as a topic
/// string or builds one with the broker's own source type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_includes_dispatch() {
    let router = Router::<MemoryBroker>::new()
        .include(ri_plain)
        .include(ri_on)
        .include(ri_batch)
        .include(ri_batch_on);

    let app = RustStream::new(AppInfo::new("ri", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["ri-plain", "ri-on", "ri-batch", "ri-batch-on"]).await;
}

#[subscriber("rc-plain")]
async fn rc_plain(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(MemorySource::new("rc-on"))]
async fn rc_on(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("rc-batch")]
async fn rc_batch(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[subscriber(MemorySource::new("rc-batch-on"))]
async fn rc_batch_on(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

/// The same four registrations decode through a chain codec named once with `with_codec`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_includes_dispatch() {
    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(rc_plain)
        .include(rc_on)
        .include(rc_batch)
        .include(rc_batch_on);

    let app = RustStream::new(AppInfo::new("rc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["rc-plain", "rc-on", "rc-batch", "rc-batch-on"]).await;
}

#[subscriber("rm-a")]
async fn rm_a(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("rm-b")]
async fn rm_b(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// `merge` keeps both routers' registrations (and their metadata order: own first, merged
/// after); a router-scope `layer` on the merged router still dispatches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_router_dispatches_and_collects_metadata() {
    let merged = Router::<MemoryBroker>::new().include(rm_a).merge(
        Router::<MemoryBroker>::new()
            .layer(TracingLayer::default())
            .include(rm_b),
    );

    let names: Vec<_> = merged.handlers().into_iter().map(|m| m.name).collect();
    assert_eq!(names, ["rm-a", "rm-b"]);

    let app = RustStream::new(AppInfo::new("rm", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(merged));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["rm-a", "rm-b"]).await;
}
