//! Integration coverage for the `include` family on `BrokerScope`, in both codec forms.
//!
//! `with_broker_codec` sets a scope default codec, switching every `include*` call to the
//! `BrokerScope<B, L, C: Codec>` impl block; the bare `with_broker` path uses the default-codec
//! block (`C = ()`). The own-source default-codec variants are covered elsewhere; the explicit-
//! source `_on` variants of both blocks were not. This drives every `_on` form (plus batch and
//! batch-publishing) through one codec scope and one default-codec scope, end to end.
#![cfg(feature = "macros")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::wait_for;
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::{Name, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Receipt {
    id: u32,
}

static PLAIN_ON: AtomicUsize = AtomicUsize::new(0);
static BATCH: AtomicUsize = AtomicUsize::new(0);
static BATCH_ON: AtomicUsize = AtomicUsize::new(0);
static POUT: AtomicUsize = AtomicUsize::new(0);
static POUT_ON: AtomicUsize = AtomicUsize::new(0);
static BPOUT: AtomicUsize = AtomicUsize::new(0);
static BPOUT_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber("sc-plain-on")]
async fn plain_on(_o: &Order) -> HandlerResult {
    PLAIN_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch("sc-batch"))]
async fn batch(orders: &[Order]) -> HandlerResult {
    BATCH.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch("sc-batch-ignored"))]
async fn batch_on(orders: &[Order]) -> HandlerResult {
    BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("sc-pin", publish("sc-pout"))]
async fn relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("sc-pin-ignored", publish("sc-pout-on"))]
async fn relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber(batch("sc-bpin"), publish("sc-bpout"))]
async fn batch_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber(batch("sc-bpin-ignored"), publish("sc-bpout-on"))]
async fn batch_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("sc-pout")]
async fn pout_check(_r: &Receipt) -> HandlerResult {
    POUT.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("sc-pout-on")]
async fn pout_on_check(_r: &Receipt) -> HandlerResult {
    POUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("sc-bpout")]
async fn bpout_check(_r: &Receipt) -> HandlerResult {
    BPOUT.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("sc-bpout-on")]
async fn bpout_on_check(_r: &Receipt) -> HandlerResult {
    BPOUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// One codec scope, every non-`include` scope-codec variant mounted: `include_on`,
/// `include_batch`, `include_batch_on`, `include_publishing`, `include_publishing_on`,
/// `include_batch_publishing`, and `include_batch_publishing_on`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_codec_include_family_dispatches() {
    let broker = MemoryBroker::new();
    let driver = broker.clone().publisher();
    let reply_broker = broker.clone();

    let app =
        RustStream::new(AppInfo::new("sc", "0.1.0")).with_broker_codec(broker, JsonCodec, |b| {
            b.include_on(Name::new("sc-plain-on"), plain_on);
            b.include_batch(batch);
            b.include_batch_on(Name::new("sc-batch-on"), batch_on);
            b.include_publishing(relay, TypedPublisher::new(reply_broker.publisher()));
            b.include_publishing_on(
                Name::new("sc-pin-on"),
                relay_on,
                TypedPublisher::new(reply_broker.publisher()),
            );
            b.include_batch_publishing(batch_relay, TypedPublisher::new(reply_broker.publisher()));
            b.include_batch_publishing_on(
                Name::new("sc-bpin-on"),
                batch_relay_on,
                TypedPublisher::new(reply_broker.publisher()),
            );
            b.include(pout_check);
            b.include(pout_on_check);
            b.include(bpout_check);
            b.include(bpout_on_check);
        });

    // `start` resolves only once subscriptions are open, so one publish per topic suffices.
    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Order { id: 1 }).unwrap();
    let topics = [
        "sc-plain-on",
        "sc-batch",
        "sc-batch-on",
        "sc-pin",
        "sc-pin-on",
        "sc-bpin",
        "sc-bpin-on",
    ];
    let counters: [&AtomicUsize; 7] = [
        &PLAIN_ON, &BATCH, &BATCH_ON, &POUT, &POUT_ON, &BPOUT, &BPOUT_ON,
    ];

    for topic in topics {
        driver
            .publish(OutgoingMessage::new(topic, &payload))
            .await
            .expect("publish");
    }
    wait_for(
        || counters.iter().all(|c| c.load(Ordering::SeqCst) >= 1),
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static D_PLAIN_ON: AtomicUsize = AtomicUsize::new(0);
static D_BATCH_ON: AtomicUsize = AtomicUsize::new(0);
static D_POUT_ON: AtomicUsize = AtomicUsize::new(0);
static D_BPOUT_ON: AtomicUsize = AtomicUsize::new(0);

#[subscriber("d-plain-on")]
async fn d_plain_on(_o: &Order) -> HandlerResult {
    D_PLAIN_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber(batch("d-batch-ignored"))]
async fn d_batch_on(orders: &[Order]) -> HandlerResult {
    D_BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("d-pin-ignored", publish("d-pout-on"))]
async fn d_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber(batch("d-bpin-ignored"), publish("d-bpout-on"))]
async fn d_batch_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("d-pout-on")]
async fn d_pout_on_check(_r: &Receipt) -> HandlerResult {
    D_POUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("d-bpout-on")]
async fn d_bpout_on_check(_r: &Receipt) -> HandlerResult {
    D_BPOUT_ON.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// The default-codec block's explicit-source variants: `include_on`, `include_batch_on`,
/// `include_publishing_on`, and `include_batch_publishing_on` (the own-source forms are covered
/// by the other integration tests).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_include_on_family_dispatches() {
    let broker = MemoryBroker::new();
    let driver = broker.clone().publisher();
    let reply_broker = broker.clone();

    let app = RustStream::new(AppInfo::new("dsc", "0.1.0")).with_broker(broker, |b| {
        b.include_on(Name::new("d-plain-on"), d_plain_on);
        b.include_batch_on(Name::new("d-batch-on"), d_batch_on);
        b.include_publishing_on(
            Name::new("d-pin-on"),
            d_relay_on,
            TypedPublisher::new(reply_broker.publisher()),
        );
        b.include_batch_publishing_on(
            Name::new("d-bpin-on"),
            d_batch_relay_on,
            TypedPublisher::new(reply_broker.publisher()),
        );
        b.include(d_pout_on_check);
        b.include(d_bpout_on_check);
    });

    // `start` resolves only once subscriptions are open, so one publish per topic suffices.
    let running = app.start().await.expect("startup failed");

    let payload = serde_json::to_vec(&Order { id: 1 }).unwrap();
    let topics = ["d-plain-on", "d-batch-on", "d-pin-on", "d-bpin-on"];
    let counters: [&AtomicUsize; 4] = [&D_PLAIN_ON, &D_BATCH_ON, &D_POUT_ON, &D_BPOUT_ON];

    for topic in topics {
        driver
            .publish(OutgoingMessage::new(topic, &payload))
            .await
            .expect("publish");
    }
    wait_for(
        || counters.iter().all(|c| c.load(Ordering::SeqCst) >= 1),
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}
