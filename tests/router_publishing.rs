//! Integration tests for the `Router` publishing include family (single-message and batch),
//! in both codec forms: the default codec and a chain codec set with `with_codec`. Replies are
//! verified end to end by plain subscribers on the reply topics.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::handler_signal;
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream, TypedPublisher};
use ruststream::{Name, OutgoingMessage, Publisher, subscriber};
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

/// Publishes orders to each ingress topic until every reply counter is non-zero (subscriptions
/// open inside `run()`, so early publishes are lost), bounded by a hang guard.
async fn drive_until_replied(
    publisher: &impl Publisher<Error = std::convert::Infallible>,
    topics: &[&str],
    counters: &[&AtomicUsize],
    signal: &Notify,
) {
    let payload = order_bytes(1);
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for topic in topics {
                let _ = publisher
                    .publish(OutgoingMessage::new(topic, &payload))
                    .await;
            }
            handler_signal(signal).await;
            if counters.iter().all(|c| c.load(Ordering::SeqCst) >= 1) {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "not every publishing form replied");
}

static RP_OUT: AtomicUsize = AtomicUsize::new(0);
static RP_OUT_ON: AtomicUsize = AtomicUsize::new(0);
static RP_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

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
    RP_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("rp-out-on")]
async fn rp_check_on(_r: &Receipt) -> HandlerResult {
    RP_OUT_ON.fetch_add(1, Ordering::SeqCst);
    RP_NOTIFY.notify_one();
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

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_replied(
        &publisher,
        &["rp-in", "rp-in-on"],
        &[&RP_OUT, &RP_OUT_ON],
        &RP_NOTIFY,
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static RPC_OUT: AtomicUsize = AtomicUsize::new(0);
static RPC_OUT_ON: AtomicUsize = AtomicUsize::new(0);
static RPC_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

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
    RPC_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("rpc-out-on")]
async fn rpc_check_on(_r: &Receipt) -> HandlerResult {
    RPC_OUT_ON.fetch_add(1, Ordering::SeqCst);
    RPC_NOTIFY.notify_one();
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

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_replied(
        &publisher,
        &["rpc-in", "rpc-in-on"],
        &[&RPC_OUT, &RPC_OUT_ON],
        &RPC_NOTIFY,
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static BP_OUT: AtomicUsize = AtomicUsize::new(0);
static BP_OUT_ON: AtomicUsize = AtomicUsize::new(0);
static BP_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

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
    BP_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("bp-out-on")]
async fn bp_check_on(_r: &Receipt) -> HandlerResult {
    BP_OUT_ON.fetch_add(1, Ordering::SeqCst);
    BP_NOTIFY.notify_one();
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

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_replied(
        &publisher,
        &["bp-in", "bp-in-on"],
        &[&BP_OUT, &BP_OUT_ON],
        &BP_NOTIFY,
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static BPC_OUT: AtomicUsize = AtomicUsize::new(0);
static BPC_OUT_ON: AtomicUsize = AtomicUsize::new(0);
static BPC_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

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
    BPC_NOTIFY.notify_one();
    HandlerResult::Ack
}

#[subscriber("bpc-out-on")]
async fn bpc_check_on(_r: &Receipt) -> HandlerResult {
    BPC_OUT_ON.fetch_add(1, Ordering::SeqCst);
    BPC_NOTIFY.notify_one();
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

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    drive_until_replied(
        &publisher,
        &["bpc-in", "bpc-in-on"],
        &[&BPC_OUT, &BPC_OUT_ON],
        &BPC_NOTIFY,
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

// A static, app-wide publish middleware that stamps a header onto every reply. Used to prove the
// app's `publish_layer` chain reaches a router-mounted publishing handler (it previously did not -
// router publishing handlers were built with an empty pipeline).
#[derive(Clone)]
struct StampApp;

impl ruststream::runtime::PublishMiddleware for StampApp {
    fn on_publish<'a, N: ruststream::runtime::PublishPipeline>(
        &'a self,
        out: &'a mut ruststream::runtime::Outgoing<'a>,
        next: ruststream::runtime::PublishNext<'a, N>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
    > {
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

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // Re-publish until the reply lands (subscriptions open inside run()).
    let payload = order_bytes(1);
    let driven = tokio::time::timeout(Duration::from_secs(5), async {
        let notified = RL_NOTIFY.notified();
        tokio::pin!(notified);
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("rl-in", &payload))
                .await;
            tokio::select! {
                () = &mut notified => break,
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await;
    assert!(driven.is_ok(), "router publishing handler never replied");

    assert_eq!(
        *RL_STAMPED.lock().unwrap(),
        Some(true),
        "the app-wide publish_layer must reach a router-mounted publishing handler"
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
