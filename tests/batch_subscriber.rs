//! Integration tests for the batch subscriber pipeline: the `#[subscriber(batch(..))]` form,
//! `include_batch` mounting, per-element decode failures, and the `Buffered` adapter.
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so each
//! message is published exactly once; the tests wait on the handlers' recorded state.
#![cfg(feature = "macros")]

mod common;

use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::wait_for;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream, TypedPublisher};
use ruststream::testing::expect_published;
use ruststream::{Buffered, Name, OutgoingMessage, Publisher, nonzero, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

static BATCHES: Mutex<Vec<Vec<u32>>> = Mutex::new(Vec::new());

/// Settles a whole page of orders at once.
#[subscriber(batch("orders"))]
async fn bill(orders: &[Order]) -> HandlerResult {
    BATCHES
        .lock()
        .unwrap()
        .push(orders.iter().map(|o| o.id).collect());
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_macro_def_receives_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(bill));

    let running = app.start().await.expect("startup failed");

    // The three publishes may buffer into one batch or arrive split across several.
    for id in 0..3u32 {
        publisher
            .publish(OutgoingMessage::new("orders", &order_bytes(id)))
            .await
            .expect("publish failed");
    }
    wait_for(
        || BATCHES.lock().unwrap().iter().map(Vec::len).sum::<usize>() >= 3,
        Duration::from_secs(5),
    )
    .await;

    // Nothing is dropped (the subscription was open before the first publish), so the flattened
    // stream is exactly the publish order.
    let flattened: Vec<u32> = BATCHES.lock().unwrap().iter().flatten().copied().collect();
    assert_eq!(flattened, vec![0, 1, 2], "deliveries out of publish order");
    assert!(
        BATCHES
            .lock()
            .unwrap()
            .iter()
            .all(|batch| !batch.is_empty()),
        "batches must not be empty",
    );

    running.shutdown().await.expect("graceful shutdown failed");
}

static GOOD_IDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// Records the ids that survived decoding.
#[subscriber(batch("mixed"))]
async fn sift(orders: &[Order]) -> HandlerResult {
    GOOD_IDS.lock().unwrap().extend(orders.iter().map(|o| o.id));
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undecodable_elements_never_reach_the_handler() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(sift));

    let running = app.start().await.expect("startup failed");

    publisher
        .publish(OutgoingMessage::new("mixed", &order_bytes(1)))
        .await
        .expect("publish failed");
    publisher
        .publish(OutgoingMessage::new("mixed", b"not json"))
        .await
        .expect("publish failed");
    publisher
        .publish(OutgoingMessage::new("mixed", &order_bytes(2)))
        .await
        .expect("publish failed");
    wait_for(
        || {
            let seen = GOOD_IDS.lock().unwrap();
            seen.contains(&1) && seen.contains(&2)
        },
        Duration::from_secs(5),
    )
    .await;

    // The undecodable element is dropped individually, never failing the batch around it: exactly
    // the two decodable ids reach the handler, in publish order.
    let ids = GOOD_IDS.lock().unwrap().clone();
    assert_eq!(ids, vec![1, 2], "unexpected ids reached the handler");

    running.shutdown().await.expect("graceful shutdown failed");
}

static BUFFERED_SEEN: AtomicUsize = AtomicUsize::new(0);

/// A handler mounted on a `Buffered`-wrapped source directly in the macro. The macro recovers
/// the source type from the constructor path, so a generic source spells its parameter
/// (turbofish).
#[subscriber(batch(Buffered::<Name>::new(Name::new("events")).max_size(nonzero!(2))))]
async fn drain(events: &[Order]) -> HandlerResult {
    BUFFERED_SEEN.fetch_add(events.len(), Ordering::SeqCst);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_adapter_batches_plain_subscribers_via_router() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // Mounted through the Router path to cover include_batch there as well.
    let router = Router::<MemoryBroker>::new().include_batch(drain);
    let app = RustStream::new(AppInfo::new("events", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let running = app.start().await.expect("startup failed");

    publisher
        .publish(OutgoingMessage::new("events", &order_bytes(7)))
        .await
        .expect("publish failed");
    wait_for(
        || BUFFERED_SEEN.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}

static SETTLED: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static RETRIED_ONCE: AtomicBool = AtomicBool::new(false);

/// Retries order 11 on first sight; settles everything else, per element.
#[subscriber(batch("pages"))]
async fn reconcile(orders: &[Order]) -> Vec<HandlerResult> {
    orders
        .iter()
        .map(|o| {
            if o.id == 11 && !RETRIED_ONCE.swap(true, Ordering::SeqCst) {
                HandlerResult::retry()
            } else {
                SETTLED.lock().unwrap().push(o.id);
                HandlerResult::Ack
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_element_outcomes_retry_individually() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("pages", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(reconcile));

    let running = app.start().await.expect("startup failed");

    // The page is published exactly once, so the retry accounting below is deterministic.
    for id in [10u32, 11, 12] {
        publisher
            .publish(OutgoingMessage::new("pages", &order_bytes(id)))
            .await
            .expect("publish failed");
    }
    wait_for(
        || {
            let settled = SETTLED.lock().unwrap();
            [10, 11, 12].iter().all(|id| settled.contains(id))
        },
        Duration::from_secs(5),
    )
    .await;

    // 11 was retried exactly once and settled only on redelivery; 10 and 12 settled first try.
    assert!(RETRIED_ONCE.load(Ordering::SeqCst));
    let settled = SETTLED.lock().unwrap().clone();
    for id in [10u32, 11, 12] {
        assert_eq!(
            settled.iter().filter(|s| **s == id).count(),
            1,
            "{id} must settle exactly once; settled: {settled:?}",
        );
    }

    running.shutdown().await.expect("graceful shutdown failed");
}

#[derive(Debug, Serialize, Deserialize)]
struct Confirmation {
    id: u32,
    accepted: bool,
}

/// Confirms a page of orders. The Result form gives explicit ack control; the whole-batch
/// rejection path is covered by the runtime unit tests.
#[subscriber(batch("requests"), publish("confirmations"))]
async fn confirm(orders: &[Order]) -> Result<Vec<Confirmation>, HandlerResult> {
    Ok(orders
        .iter()
        .map(|o| Confirmation {
            id: o.id,
            accepted: true,
        })
        .collect())
}

/// The plain reply form: every page is confirmed (compile coverage for `-> Vec<Reply>`).
#[subscriber(batch("requests"), publish("audit"))]
async fn audit(orders: &[Order]) -> Vec<Confirmation> {
    orders
        .iter()
        .map(|o| Confirmation {
            id: o.id,
            accepted: true,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_replies_publish_transactionally() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let observer = ruststream::Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible");

    let replies = TypedPublisher::new(broker.publisher()).transactional();
    let app = RustStream::new(AppInfo::new("confirmations", "0.1.0"))
        .with_broker(broker, |b| b.include_batch_publishing(confirm, replies));

    let running = app.start().await.expect("startup failed");

    publisher
        .publish(OutgoingMessage::new("requests", &order_bytes(7)))
        .await
        .expect("publish failed");
    // expect_published polls under its own deadline and returns whatever arrived by then.
    let confirmed = expect_published(&observer, "confirmations", 1, Duration::from_secs(5)).await;
    assert!(!confirmed.is_empty(), "no confirmation arrived");
    for raw in &confirmed {
        let confirmation: Confirmation = serde_json::from_slice(raw.payload()).unwrap();
        assert_eq!(confirmation.id, 7);
        assert!(confirmation.accepted);
    }

    running.shutdown().await.expect("graceful shutdown failed");
}

#[test]
fn batch_publishing_def_records_metadata() {
    let broker = MemoryBroker::new();
    let replies = TypedPublisher::new(broker.publisher());
    let app = RustStream::new(AppInfo::new("audit", "0.1.0"))
        .with_broker(broker, |b| b.include_batch_publishing(audit, replies));

    assert_eq!(app.handlers().len(), 1);
    assert_eq!(app.handlers()[0].name, "requests");
    assert!(
        app.handlers()[0]
            .output_type
            .is_some_and(|t| t.contains("Confirmation")),
    );
}

#[test]
fn batch_def_records_metadata() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(bill));

    assert_eq!(app.handlers().len(), 1);
    assert_eq!(app.handlers()[0].name, "orders");
    assert_eq!(
        app.handlers()[0].description.as_deref(),
        Some("Settles a whole page of orders at once."),
    );
}

/// Typed application state read from a batch handler: the multiplier is produced at startup and
/// reaches the whole-batch handler through `ctx.state()`, the same as a single-message handler.
#[derive(Clone, Copy)]
struct Tally {
    multiplier: u32,
}

static SCALED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

#[subscriber(batch("scale"))]
async fn scale(orders: &[Order], ctx: &mut Context<'_, (), Tally>) -> HandlerResult {
    let multiplier = ctx.state().multiplier;
    SCALED
        .lock()
        .unwrap()
        .extend(orders.iter().map(|o| o.id * multiplier));
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_handler_reads_typed_state() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(|()| async { Ok::<_, std::convert::Infallible>(Tally { multiplier: 10 }) })
        .with_broker(broker, |b| b.include_batch(scale));

    let running = app.start().await.expect("startup failed");

    for id in 1..4u32 {
        publisher
            .publish(OutgoingMessage::new("scale", &order_bytes(id)))
            .await
            .expect("publish failed");
    }
    wait_for(|| SCALED.lock().unwrap().len() >= 3, Duration::from_secs(5)).await;

    // Each id was multiplied by the state's multiplier (10), proving the handler read typed state.
    let scaled = SCALED.lock().unwrap().clone();
    assert!(
        scaled.iter().all(|n| n % 10 == 0),
        "every value must be a multiple of the state multiplier; got {scaled:?}",
    );
    assert!(scaled.contains(&10) && scaled.contains(&20) && scaled.contains(&30));

    running.shutdown().await.expect("graceful shutdown failed");
}
