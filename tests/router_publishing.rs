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

use common::{Order, Receipt, wait_for};
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext, PublishTransform};
use ruststream::{BuildContext, Field};
use tokio::sync::Notify;

/// Publishes an order once to each ingress topic (the app is already started, so the
/// subscriptions are open and every publish lands), then waits until every reply counter is
/// non-zero.
async fn publish_and_await_replies(
    publisher: &impl Publisher,
    topics: &[&str],
    counters: &[&AtomicUsize],
) {
    for topic in topics {
        publisher
            .message(&Order { id: 1 })
            .to(*topic)
            .publish()
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

#[subscriber("rp-in-on", publish("rp-out-on"))]
async fn rp_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rp-out")]
async fn rp_check(_r: &Receipt) -> HandlerOutcome {
    RP_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[subscriber("rp-out-on")]
async fn rp_check_on(_r: &Receipt) -> HandlerOutcome {
    RP_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// Default-codec `include` on the publishing form, twice over: replies reach the reply topics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include(rp_relay)
        .publisher(Publish)
        .build()
        .include(rp_relay_on)
        .publisher(Publish)
        .build();

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

#[subscriber("rpc-in-on", publish("rpc-out-on"))]
async fn rpc_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("rpc-out")]
async fn rpc_check(_r: &Receipt) -> HandlerOutcome {
    RPC_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[subscriber("rpc-out-on")]
async fn rpc_check_on(_r: &Receipt) -> HandlerOutcome {
    RPC_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// Chain-codec `include` on the publishing form: the input decodes with the `with_codec` codec,
/// the reply goes through the publisher's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(rpc_relay)
        .publisher(Publish)
        .build()
        .include(rpc_relay_on)
        .publisher(Publish)
        .build();

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
    BP_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[subscriber("bp-out-on")]
async fn bp_check_on(_r: &Receipt) -> HandlerOutcome {
    BP_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// Default-codec `include` on the batch publishing form: every batch element is republished to
/// the reply topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_router_batch_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include(bp_relay.batch(nonzero!(8)).publisher(Publish).build())
        .include(bp_relay_on.batch(nonzero!(8)).publisher(Publish).build());

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
    BPC_OUT.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[subscriber("bpc-out-on")]
async fn bpc_check_on(_r: &Receipt) -> HandlerOutcome {
    BPC_OUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// Chain-codec `include` on the batch publishing form: elements decode with the `with_codec`
/// codec, replies go through the publisher's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chain_codec_router_batch_publishing_replies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(bpc_relay.batch(nonzero!(8)).publisher(Publish).build())
        .include(bpc_relay_on.batch(nonzero!(8)).publisher(Publish).build());

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

static RL_STAMPED: LazyLock<std::sync::Mutex<Option<bool>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));
static RL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("rl-out")]
async fn rl_check(_r: &Receipt, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerOutcome {
    *RL_STAMPED.lock().unwrap() = Some(ctx.headers().get("x-app").is_some());
    RL_NOTIFY.notify_one();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_publishing_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include(rl_relay)
        .publisher(Publish)
        .build();

    let app = RustStream::new(AppInfo::new("rl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(broker, |b| {
            b.include_router(router);
            b.include(rl_check);
        });

    let running = app.start().await.expect("startup failed");

    publisher
        .message(&Order { id: 1 })
        .to("rl-in")
        .publish()
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

// The same on the BATCH router-publishing path: the app's publish_layer must reach a
// router-mounted batch publishing handler.
#[subscriber("bl-in", publish("bl-out"))]
async fn bl_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

static BL_STAMPED: LazyLock<std::sync::Mutex<Option<bool>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));
static BL_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("bl-out")]
async fn bl_check(_r: &Receipt, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerOutcome {
    *BL_STAMPED.lock().unwrap() = Some(ctx.headers().get("x-app").is_some());
    BL_NOTIFY.notify_one();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_batch_publishing_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router =
        Router::<MemoryBroker>::new().include(bl_relay.batch(nonzero!(8)).publisher(Publish).build());

    let app = RustStream::new(AppInfo::new("bl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(broker, |b| {
            b.include_router(router);
            b.include(bl_check);
        });

    let running = app.start().await.expect("startup failed");

    publisher
        .message(&Order { id: 1 })
        .to("bl-in")
        .publish()
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

static TC_CORR: LazyLock<std::sync::Mutex<Option<String>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));
static TC_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[subscriber("tc-out")]
async fn tc_check(_r: &Receipt, ctx: &mut ruststream::runtime::Context<'_>) -> HandlerOutcome {
    *TC_CORR.lock().unwrap() = ctx.headers().correlation_id().map(str::to_owned);
    TC_NOTIFY.notify_one();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_publishing_threads_typed_delivery_context() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let router = Router::<MemoryBroker>::new()
        .include(tc_relay)
        .publisher(Publish)
        .transform(PropagateCorrelation)
        .build();

    let app = RustStream::new(AppInfo::new("tc", "0.1.0")).with_broker(broker, |b| {
        b.include_router(router);
        b.include(tc_check);
    });

    let running = app.start().await.expect("startup failed");

    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "trace-xyz");
    publisher
        .message(&Order { id: 1 })
        .with_headers(headers)
        .to("tc-in")
        .publish()
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
