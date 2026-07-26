//! Integration tests for the `Router` publishing include family (single-message and batch),
//! in both codec forms: the default codec and a chain codec set with `with_codec`. Replies are
//! verified end to end by plain subscribers on the reply topics.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::wait_for;
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::runtime::{
    AppInfo, HandlerResult, Outgoing, PublishContext, PublishTransform, Router, RustStream,
    TypedPublisher,
};
use ruststream::{
    BuildContext, Field, Headers, IncomingMessage, Name, OutgoingMessage, Publisher, subscriber,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Receipt {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

/// Publishes an order once to each ingress topic (the app is already started, so the
/// subscriptions are open and every publish lands), then waits until every reply counter is
/// non-zero.
async fn publish_and_await_replies(
    publisher: &impl Publisher<Error = std::convert::Infallible>,
    topics: &[&str],
    counters: &[&AtomicUsize],
) {
    let payload = order_bytes(1);
    for topic in topics {
        publisher
            .publish(OutgoingMessage::new(topic, &payload))
            .await
            .expect("publish");
    }
    wait_for(
        || counters.iter().all(|c| c.load(Ordering::SeqCst) >= 1),
        Duration::from_secs(5),
    )
    .await;
}

static RP_OUT: AtomicUsize = AtomicUsize::new(0);
static RP_OUT_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber("rp-in", publish("rp-out"))]
async fn rp_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rp-ignored", publish("rp-out-on"))]
async fn rp_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rp-out")]
async fn rp_check(_r: &Receipt) -> HandlerResult {
    RP_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("rp-out-on")]
async fn rp_check_on(_r: &Receipt) -> HandlerResult {
    RP_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// Default-codec `include_publishing` / `include_publishing_on`: replies reach the reply topics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include_publishing(rp_relay, TypedPublisher::new(broker.publisher()))
        .include_publishing_on(
            Name::new("rp-in-on"),
            rp_relay_on,
            TypedPublisher::new(broker.publisher()),
        );

    let app = RustStream::new(AppInfo::new("rp", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
        b.include(rp_check);
        b.include(rp_check_on);
    });

    let running = app.start().await.expect("startup failed");

    publish_and_await_replies(&publisher, &["rp-in", "rp-in-on"], &[&RP_OUT, &RP_OUT_ON]).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static RPC_OUT: AtomicUsize = AtomicUsize::new(0);
static RPC_OUT_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber("rpc-in", publish("rpc-out"))]
async fn rpc_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rpc-ignored", publish("rpc-out-on"))]
async fn rpc_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rpc-out")]
async fn rpc_check(_r: &Receipt) -> HandlerResult {
    RPC_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("rpc-out-on")]
async fn rpc_check_on(_r: &Receipt) -> HandlerResult {
    RPC_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// Chain-codec `include_publishing` / `include_publishing_on`: the input decodes with the
/// `with_codec` codec, the reply goes through the publisher's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include_publishing(rpc_relay, TypedPublisher::new(broker.publisher()))
        .include_publishing_on(
            Name::new("rpc-in-on"),
            rpc_relay_on,
            TypedPublisher::new(broker.publisher()),
        );

    let app = RustStream::new(AppInfo::new("rpc", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
        b.include(rpc_check);
        b.include(rpc_check_on);
    });

    let running = app.start().await.expect("startup failed");

    publish_and_await_replies(
        &publisher,
        &["rpc-in", "rpc-in-on"],
        &[&RPC_OUT, &RPC_OUT_ON],
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static BP_OUT: AtomicUsize = AtomicUsize::new(0);
static BP_OUT_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber(batch("bp-in"), publish("bp-out"))]
async fn bp_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber(batch("bp-ignored"), publish("bp-out-on"))]
async fn bp_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bp-out")]
async fn bp_check(_r: &Receipt) -> HandlerResult {
    BP_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("bp-out-on")]
async fn bp_check_on(_r: &Receipt) -> HandlerResult {
    BP_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// Default-codec `include_batch_publishing` / `include_batch_publishing_on`: every batch element
/// is republished to the reply topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_batch_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include_batch_publishing(bp_relay, TypedPublisher::new(broker.publisher()))
        .include_batch_publishing_on(
            Name::new("bp-in-on"),
            bp_relay_on,
            TypedPublisher::new(broker.publisher()),
        );

    let app = RustStream::new(AppInfo::new("bp", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
        b.include(bp_check);
        b.include(bp_check_on);
    });

    let running = app.start().await.expect("startup failed");

    publish_and_await_replies(&publisher, &["bp-in", "bp-in-on"], &[&BP_OUT, &BP_OUT_ON]).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static BPC_OUT: AtomicUsize = AtomicUsize::new(0);
static BPC_OUT_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber(batch("bpc-in"), publish("bpc-out"))]
async fn bpc_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber(batch("bpc-ignored"), publish("bpc-out-on"))]
async fn bpc_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("bpc-out")]
async fn bpc_check(_r: &Receipt) -> HandlerResult {
    BPC_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("bpc-out-on")]
async fn bpc_check_on(_r: &Receipt) -> HandlerResult {
    BPC_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// Chain-codec `include_batch_publishing` / `include_batch_publishing_on`: elements decode with
/// the `with_codec` codec, replies go through the publisher's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_batch_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include_batch_publishing(bpc_relay, TypedPublisher::new(broker.publisher()))
        .include_batch_publishing_on(
            Name::new("bpc-in-on"),
            bpc_relay_on,
            TypedPublisher::new(broker.publisher()),
        );

    let app = RustStream::new(AppInfo::new("bpc", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
        b.include(bpc_check);
        b.include(bpc_check_on);
    });

    let running = app.start().await.expect("startup failed");

    publish_and_await_replies(
        &publisher,
        &["bpc-in", "bpc-in-on"],
        &[&BPC_OUT, &BPC_OUT_ON],
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// A static, app-wide publish middleware that stamps a header onto every reply. Used to prove the
// app's `publish_layer` chain reaches a router-mounted publishing handler (it previously did not -
// router publishing handlers were built with an empty pipeline).
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

static RL_STAMPED: LazyLock<std::sync::Mutex<Option<bool>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));
static RL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("rl-out")]
async fn rl_check(_r: &Receipt, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerResult {
    *RL_STAMPED.lock().unwrap() = Some(ctx.headers().get("x-app").is_some());
    RL_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_publishing_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include_publishing(rl_relay, TypedPublisher::new(broker.publisher()));

    let app = RustStream::new(AppInfo::new("rl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(broker, |b| {
            b.include_router(router);
            b.include(rl_check);
        });

    let running = app.start().await.expect("startup failed");

    let payload = order_bytes(1);
    publisher
        .publish(OutgoingMessage::new("rl-in", &payload))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), RL_NOTIFY.notified())
        .await
        .expect("router publishing handler never replied");

    assert_eq!(
        *RL_STAMPED.lock().unwrap(),
        Some(true),
        "the app-wide publish_layer must reach a router-mounted publishing handler"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

// The same fix on the BATCH router-publishing path: the app's publish_layer must reach a
// router-mounted batch publishing handler (previously built with an empty pipeline).
#[subscriber(batch("bl-in"), publish("bl-out"))]
async fn bl_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

static BL_STAMPED: LazyLock<std::sync::Mutex<Option<bool>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));
static BL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("bl-out")]
async fn bl_check(_r: &Receipt, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerResult {
    *BL_STAMPED.lock().unwrap() = Some(ctx.headers().get("x-app").is_some());
    BL_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_batch_publishing_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include_batch_publishing(bl_relay, TypedPublisher::new(broker.publisher()));

    let app = RustStream::new(AppInfo::new("bl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(broker, |b| {
            b.include_router(router);
            b.include(bl_check);
        });

    let running = app.start().await.expect("startup failed");

    let payload = order_bytes(1);
    publisher
        .publish(OutgoingMessage::new("bl-in", &payload))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), BL_NOTIFY.notified())
        .await
        .expect("router batch publishing handler never replied");

    assert_eq!(
        *BL_STAMPED.lock().unwrap(),
        Some(true),
        "the app-wide publish_layer must reach a router-mounted batch publishing handler"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

// A typed delivery context on a ROUTER-mounted publishing handler. The old SubscribeRoute-based
// router path forced the context to `()`; the deferred PublishingRoute threads `D::Context`, so a
// publish layer can read the delivery by key. This would not compile on the pre-#113 router.
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

static TC_CORR: LazyLock<std::sync::Mutex<Option<String>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));
static TC_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("tc-out")]
async fn tc_check(_r: &Receipt, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerResult {
    *TC_CORR.lock().unwrap() = ctx.headers().correlation_id().map(str::to_owned);
    TC_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_publishing_threads_typed_delivery_context() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new().include_publishing(
        tc_relay,
        TypedPublisher::new(broker.publisher()).transform(PropagateCorrelation),
    );

    let app = RustStream::new(AppInfo::new("tc", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
        b.include(tc_check);
    });

    let running = app.start().await.expect("startup failed");

    let payload = order_bytes(1);
    let mut headers = Headers::new();
    headers.insert("correlation-id", "trace-xyz");
    publisher
        .publish(OutgoingMessage::new("tc-in", &payload).with_headers(headers))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), TC_NOTIFY.notified())
        .await
        .expect("typed-context router relay never replied");

    assert_eq!(
        TC_CORR.lock().unwrap().as_deref(),
        Some("trace-xyz"),
        "a router publishing handler must thread its typed delivery context to the publish layer"
    );

    running.shutdown().await.expect("graceful shutdown failed");
}
