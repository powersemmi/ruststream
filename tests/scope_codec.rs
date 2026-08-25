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
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerResult, PublishExt, RustStream, TypedPublisher};
use ruststream::subscriber;
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

#[subscriber(batch("sc-batch-on"))]
async fn batch_on(orders: &[Order]) -> HandlerResult {
    BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("sc-pin", publish("sc-pout"))]
async fn relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("sc-pin-on", publish("sc-pout-on"))]
async fn relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber(batch("sc-bpin"), publish("sc-bpout"))]
async fn batch_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber(batch("sc-bpin-on"), publish("sc-bpout-on"))]
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

/// One codec scope, every scope-codec variant mounted: the plain and batch `include`s, and both
/// reply-publishing shapes, each registered twice so the scope codec is proven on every path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_codec_include_family_dispatches() {
    let broker = MemoryBroker::new();
    let driver = broker.clone().publisher();

    let app =
        RustStream::new(AppInfo::new("sc", "0.1.0")).with_broker_codec(broker, JsonCodec, |b| {
            b.include(plain_on);
            b.include(batch);
            b.include(batch_on);
            b.include(relay)
                .publisher(TypedPublisher::new(MemoryPublish));
            b.include(relay_on)
                .publisher(TypedPublisher::new(MemoryPublish));
            b.include(batch_relay)
                .publisher(TypedPublisher::new(MemoryPublish));
            b.include(batch_relay_on)
                .publisher(TypedPublisher::new(MemoryPublish));
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
            .raw(&payload)
            .to(topic)
            .publish()
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

#[subscriber(batch("d-batch-on"))]
async fn d_batch_on(orders: &[Order]) -> HandlerResult {
    D_BATCH_ON.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("d-pin-on", publish("d-pout-on"))]
async fn d_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber(batch("d-bpin-on"), publish("d-bpout-on"))]
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

/// The same family on the default-codec block: the plain and batch `include`s next to both
/// reply-publishing shapes, decoding with the default codec rather than a named one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_include_family_dispatches() {
    let broker = MemoryBroker::new();
    let driver = broker.clone().publisher();

    let app = RustStream::new(AppInfo::new("dsc", "0.1.0")).with_broker(broker, |b| {
        b.include(d_plain_on);
        b.include(d_batch_on);
        b.include(d_relay_on)
            .publisher(TypedPublisher::new(MemoryPublish));
        b.include(d_batch_relay_on)
            .publisher(TypedPublisher::new(MemoryPublish));
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
            .raw(&payload)
            .to(topic)
            .publish()
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
