//! Integration tests for the `Router` publishing include family (single-message and batch),
//! in both codec forms: the default codec and a chain codec set with `with_codec`. Replies are
//! verified end to end by plain subscribers on the reply topics.
#![cfg(all(
    feature = "macros",
    feature = "testing",
    feature = "memory",
    feature = "json"
))]

mod common;

use common::{Order, Receipt};
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext, PublishTransform};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, Field};

#[subscriber("rp-in", publish("rp-out"))]
async fn rp_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rp-in-on", publish("rp-out-on"))]
async fn rp_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rp-out")]
async fn rp_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("rp-out-on")]
async fn rp_check_on(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Default-codec `include` on the publishing form, twice over: replies reach the reply topics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_publishing_replies() {
    let router = Router::<MemoryBroker>::new()
        .include(rp_relay)
        .publisher(Publish)
        .build()
        .include(rp_relay_on)
        .publisher(Publish)
        .build();

    let app = RustStream::new(AppInfo::new("rp", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
        b.include(rp_check);
        b.include(rp_check_on);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    for topic in ["rp-in", "rp-in-on"] {
        tb.message(&Order { id: 1 })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    for reply in ["rp-out", "rp-out-on"] {
        tb.broker::<MemoryBroker>()
            .subscriber(reply)
            .assert_called_once()
            .with(&Receipt { id: 1 })
            .settled(HandlerOutcome::ack());
    }
}

#[subscriber("rpc-in", publish("rpc-out"))]
async fn rpc_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rpc-in-on", publish("rpc-out-on"))]
async fn rpc_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rpc-out")]
async fn rpc_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("rpc-out-on")]
async fn rpc_check_on(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Chain-codec `include` on the publishing form: the input decodes with the `with_codec` codec,
/// the reply goes through the publisher's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_publishing_replies() {
    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(rpc_relay)
        .publisher(Publish)
        .build()
        .include(rpc_relay_on)
        .publisher(Publish)
        .build();

    let app = RustStream::new(AppInfo::new("rpc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
        b.include(rpc_check);
        b.include(rpc_check_on);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    for topic in ["rpc-in", "rpc-in-on"] {
        tb.message(&Order { id: 1 })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    for reply in ["rpc-out", "rpc-out-on"] {
        tb.broker::<MemoryBroker>()
            .subscriber(reply)
            .assert_called_once()
            .with(&Receipt { id: 1 })
            .settled(HandlerOutcome::ack());
    }
}

#[subscriber("bp-in", publish("bp-out"))]
async fn bp_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bp-in-on", publish("bp-out-on"))]
async fn bp_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bp-out")]
async fn bp_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("bp-out-on")]
async fn bp_check_on(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Default-codec `include` on the batch publishing form: every batch element is republished to
/// the reply topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_batch_publishing_replies() {
    let router = Router::<MemoryBroker>::new()
        .include(bp_relay.batch(nonzero!(8)).publisher(Publish).build())
        .include(bp_relay_on.batch(nonzero!(8)).publisher(Publish).build());

    let app = RustStream::new(AppInfo::new("bp", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
        b.include(bp_check);
        b.include(bp_check_on);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    for topic in ["bp-in", "bp-in-on"] {
        tb.message(&Order { id: 1 })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    for reply in ["bp-out", "bp-out-on"] {
        tb.broker::<MemoryBroker>()
            .subscriber(reply)
            .assert_called_once()
            .with(&Receipt { id: 1 })
            .settled(HandlerOutcome::ack());
    }
}

#[subscriber("bpc-in", publish("bpc-out"))]
async fn bpc_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bpc-in-on", publish("bpc-out-on"))]
async fn bpc_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bpc-out")]
async fn bpc_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("bpc-out-on")]
async fn bpc_check_on(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Chain-codec `include` on the batch publishing form: elements decode with the `with_codec`
/// codec, replies go through the publisher's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_batch_publishing_replies() {
    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(bpc_relay.batch(nonzero!(8)).publisher(Publish).build())
        .include(bpc_relay_on.batch(nonzero!(8)).publisher(Publish).build());

    let app = RustStream::new(AppInfo::new("bpc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
        b.include(bpc_check);
        b.include(bpc_check_on);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    for topic in ["bpc-in", "bpc-in-on"] {
        tb.message(&Order { id: 1 })
            .to(topic)
            .publish()
            .await
            .expect("publish");
    }

    for reply in ["bpc-out", "bpc-out-on"] {
        tb.broker::<MemoryBroker>()
            .subscriber(reply)
            .assert_called_once()
            .with(&Receipt { id: 1 })
            .settled(HandlerOutcome::ack());
    }
}

// A static, app-wide publish middleware that stamps a header onto every reply. Used to prove the
// app's `publish_layer` chain reaches a router-mounted publishing handler.
#[derive(Clone)]
struct StampApp;

impl ruststream::runtime::PublishLayer for StampApp {
    fn on_publish<'a, N: ruststream::runtime::PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: ruststream::runtime::PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        out.headers_mut().insert("x-app", b"1".to_vec());
        next.run(out)
    }
}

#[subscriber("rl-in", publish("rl-out"))]
async fn rl_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rl-out")]
async fn rl_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_publishing_handlers() {
    let router = Router::<MemoryBroker>::new()
        .include(rl_relay)
        .publisher(Publish)
        .build();

    let app = RustStream::new(AppInfo::new("rl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(router);
            b.include(rl_check);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("rl-in")
        .publish()
        .await
        .expect("publish");

    // The app-wide publish_layer must reach a router-mounted publishing handler, so the reply it
    // sent carries the stamp - and still arrives at the consumer.
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("rl-out")
        .assert_called_once()
        .with(&Receipt { id: 1 })
        .with_header("x-app", b"1");
    tb.broker::<MemoryBroker>()
        .subscriber("rl-out")
        .assert_called_once()
        .with(&Receipt { id: 1 });
}

// The same on the BATCH router-publishing path: the app's publish_layer must reach a
// router-mounted batch publishing handler.
#[subscriber("bl-in", publish("bl-out"))]
async fn bl_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bl-out")]
async fn bl_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_batch_publishing_handlers() {
    let router = Router::<MemoryBroker>::new()
        .include(bl_relay.batch(nonzero!(8)).publisher(Publish).build());

    let app = RustStream::new(AppInfo::new("bl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(router);
            b.include(bl_check);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("bl-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("bl-out")
        .assert_called_once()
        .with(&Receipt { id: 1 })
        .with_header("x-app", b"1");
    tb.broker::<MemoryBroker>()
        .subscriber("bl-out")
        .assert_called_once()
        .with(&Receipt { id: 1 });
}

// A typed delivery context on a ROUTER-mounted publishing handler: the route threads
// `D::Context`, so a publish layer can read the delivery by key.
#[derive(Default)]
struct TraceCtx {
    correlation: Option<String>,
}

impl BuildContext<MemoryMessage> for TraceCtx {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            correlation: msg.headers().correlation_id().map(str::to_owned),
        }
    }
}

#[derive(Clone, Copy)]
struct Correlation;

impl Field<TraceCtx> for Correlation {
    type Value<'a> = Option<&'a str>;
    fn get(self, c: &TraceCtx) -> Option<&str> {
        c.correlation.as_deref()
    }
}

struct PropagateCorrelation;

impl PublishTransform<TraceCtx> for PropagateCorrelation {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, TraceCtx>) {
        if let Some(id) = cx.context(Correlation) {
            out.headers_mut()
                .insert("correlation-id", id.as_bytes().to_vec());
        }
    }
}

#[subscriber("tc-in", publish("tc-out"))]
async fn tc_relay(o: &Order, _ctx: &mut ruststream::runtime::Context<'_, TraceCtx>) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("tc-out")]
async fn tc_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_publishing_threads_typed_delivery_context() {
    let router = Router::<MemoryBroker>::new()
        .include(tc_relay)
        .publisher(Publish)
        .transform(PropagateCorrelation)
        .build();

    let app = RustStream::new(AppInfo::new("tc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
        b.include(tc_check);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "trace-xyz");
    tb.message(&Order { id: 1 })
        .with_headers(headers)
        .to("tc-in")
        .publish()
        .await
        .expect("publish");

    // A router publishing handler must thread its typed delivery context to the publish layer,
    // which is what lets the transform copy the correlation id onto the reply.
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("tc-out")
        .assert_called_once()
        .with_header("correlation-id", b"trace-xyz");
    tb.broker::<MemoryBroker>()
        .subscriber("tc-out")
        .assert_called_once()
        .with(&Receipt { id: 1 });
}
