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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::Order;
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryMessage, MemorySource};
use ruststream::runtime::{
    AppInfo, Context, Handler, HandlerOutcome, Layer, Router, RustStream, SubscriberSettings,
    layers::TracingLayer,
};
use ruststream::testing::TestApp;
use ruststream::{nonzero, subscriber};

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
        .include(ri_batch.batch(nonzero!(64)))
        .include(ri_batch_on.batch(nonzero!(64)));

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
        .include(rc_batch.batch(nonzero!(64)))
        .include(rc_batch_on.batch(nonzero!(64)));

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

/// A layer fixed to one message type - the shape a runtime-composed
/// [`DynStack`](ruststream::runtime::DynStack) has - has no blanket impl, so it rides one
/// registration: `.layer(..)` after an `include` wraps that registration outside its decode step.
/// The counter proves the wrapper ran, and that it saw only the handler it was named on.
#[derive(Clone)]
struct CountRaw(Arc<AtomicUsize>);

struct Counted<H>(Arc<AtomicUsize>, H);

impl<H> Layer<H> for CountRaw {
    type Handler = Counted<H>;

    fn layer(&self, inner: H) -> Counted<H> {
        Counted(Arc::clone(&self.0), inner)
    }
}

// Fixed to the broker's own message type, exactly as a `DynStack<MemoryMessage>` is: this is
// what cannot be written as a `BlanketLayer`.
impl<H: Handler<MemoryMessage>> Handler<MemoryMessage> for Counted<H> {
    async fn handle(&self, msg: &MemoryMessage, ctx: &mut Context<'_>) -> HandlerOutcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.handle(msg, ctx).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registration_layer_wraps_only_the_registration_it_follows() {
    let hits = Arc::new(AtomicUsize::new(0));
    let routes = Router::<MemoryBroker>::new()
        .include(rm_a)
        .layer(CountRaw(Arc::clone(&hits)))
        .include(rm_b);

    let app = RustStream::new(AppInfo::new("registration-layer", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(routes));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["rm-a", "rm-b"]).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the layer rides the registration named before it, and no other",
    );
}
