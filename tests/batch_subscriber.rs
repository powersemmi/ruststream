//! Integration tests for the batch subscriber pipeline: the form the macro reads off a `&[T]`
//! payload parameter, batch mounting through `include`, per-element decode failures, and the
//! `Buffered` adapter.
//!
//! The harness settles each injection before it returns, so every page below is exactly the page
//! the handler was called with - no polling for "at least one".
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{Order, Wire};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, TypedPublisher};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{Buffered, Name, nonzero, subscriber};
use serde::{Deserialize, Serialize};

/// Settles a whole page of orders at once.
#[subscriber("orders")]
async fn bill(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_macro_def_receives_batches() {
    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(bill));
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in 0..3u32 {
        tb.message(&Order { id })
            .to("orders")
            .publish()
            .await
            .expect("publish failed");
    }

    // Nothing is dropped, so the flattened stream is exactly the publish order, and every page
    // the handler was called with carried something.
    let pages: Vec<Vec<Order>> = tb.broker::<MemoryBroker>().subscriber("orders").pages();
    let flattened: Vec<u32> = pages.iter().flatten().map(|o| o.id).collect();
    assert_eq!(flattened, vec![0, 1, 2], "deliveries out of publish order");
    assert!(
        pages.iter().all(|page| !page.is_empty()),
        "batches must not be empty",
    );
}

/// Records the ids that survived decoding.
#[subscriber("mixed")]
async fn sift(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undecodable_elements_never_reach_the_handler() {
    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(sift));
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("mixed")
        .publish()
        .await
        .expect("publish failed");
    tb.message(&Wire::of(b"not json"))
        .to("mixed")
        .publish()
        .await
        .expect("publish failed");
    tb.message(&Order { id: 2 })
        .to("mixed")
        .publish()
        .await
        .expect("publish failed");

    // The undecodable element is dropped individually, never failing the batch around it: exactly
    // the two decodable ids reach the handler, in publish order.
    let received: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("mixed").received();
    let ids: Vec<u32> = received.iter().map(|o| o.id).collect();
    assert_eq!(ids, vec![1, 2], "unexpected ids reached the handler");
    tb.broker::<MemoryBroker>()
        .subscriber("mixed")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
}

/// A handler mounted on a `Buffered`-wrapped source directly in the macro. The macro recovers
/// the source type from the constructor path, so a generic source spells its parameter
/// (turbofish).
#[subscriber(Buffered::<Name>::new(Name::new("events")).max_size(nonzero!(2)))]
async fn drain(events: &[Order]) -> HandlerOutcome {
    let _ = events;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_adapter_batches_plain_subscribers_via_router() {
    // Mounted through the Router path to cover the batch form there as well.
    let router = ruststream::runtime::Router::<MemoryBroker>::new().include(drain);
    let app = RustStream::new(AppInfo::new("events", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 7 })
        .to("events")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber("events")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());
}

/// Whether order 11 has already been refused once. Held in application state, so the handler
/// reads it the way a service reads any dependency.
struct Attempts {
    retried_once: Arc<AtomicBool>,
}

/// Retries order 11 on first sight; settles everything else, per element.
#[subscriber("pages")]
async fn reconcile(orders: &[Order], ctx: &mut Context<'_, (), Attempts>) -> Vec<HandlerOutcome> {
    let retried_once = Arc::clone(&ctx.state().retried_once);
    orders
        .iter()
        .map(|o| {
            if o.id == 11 && !retried_once.swap(true, Ordering::SeqCst) {
                HandlerOutcome::retry()
            } else {
                HandlerOutcome::ack()
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_element_outcomes_retry_individually() {
    let retried_once = Arc::new(AtomicBool::new(false));
    let state_flag = Arc::clone(&retried_once);
    let app = RustStream::new(AppInfo::new("pages", "0.1.0"))
        .on_startup(move |()| {
            let retried_once = state_flag;
            async move { Ok::<_, std::convert::Infallible>(Attempts { retried_once }) }
        })
        .with_broker(MemoryBroker::new(), |b| b.include(reconcile));
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in [10u32, 11, 12] {
        tb.message(&Order { id })
            .to("pages")
            .publish()
            .await
            .expect("publish failed");
    }

    // 11 was refused once and settled only on redelivery; 10 and 12 settled first try.
    assert!(retried_once.load(Ordering::SeqCst));
    let pages: Vec<Vec<Order>> = tb.broker::<MemoryBroker>().subscriber("pages").pages();
    let seen: Vec<Vec<u32>> = pages
        .iter()
        .map(|page| page.iter().map(|o| o.id).collect())
        .collect();
    assert_eq!(seen, vec![vec![10], vec![11], vec![11], vec![12]]);
    assert_eq!(
        tb.broker::<MemoryBroker>().subscriber("pages").outcomes(),
        [Outcome::Ack, Outcome::Nack, Outcome::Ack, Outcome::Ack],
    );
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Confirmation {
    id: u32,
    accepted: bool,
}

/// Confirms a page of orders. The Result form gives explicit ack control; the whole-batch
/// rejection path is covered by the runtime unit tests.
#[subscriber("requests", publish("confirmations"))]
async fn confirm(orders: &[Order]) -> Result<Vec<Confirmation>, HandlerOutcome> {
    Ok(orders
        .iter()
        .map(|o| Confirmation {
            id: o.id,
            accepted: true,
        })
        .collect())
}

/// The plain reply form: every page is confirmed (compile coverage for `-> Vec<Reply>`).
#[subscriber("requests", publish("audit"))]
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
    let replies = TypedPublisher::new(MemoryPublish).transactional();
    let app = RustStream::new(AppInfo::new("confirmations", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).publisher(replies);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 7 })
        .to("requests")
        .publish()
        .await
        .expect("publish failed");

    // The transaction commits exactly one confirmation for the one order it carried.
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation {
            id: 7,
            accepted: true,
        });
}

#[test]
fn batch_publishing_def_records_metadata() {
    let broker = MemoryBroker::new();
    let replies = TypedPublisher::new(MemoryPublish);
    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        b.include(audit).publisher(replies);
    });

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
    let app =
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| b.include(bill));

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

/// Scales each order by the multiplier it read off application state and republishes it, so what
/// the state contributed is visible on the wire.
#[subscriber("scale", publish("scaled"))]
async fn scale(orders: &[Order], ctx: &mut Context<'_, (), Tally>) -> Vec<Order> {
    let multiplier = ctx.state().multiplier;
    orders
        .iter()
        .map(|o| Order {
            id: o.id * multiplier,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_handler_reads_typed_state() {
    let app = RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Tally { multiplier: 10 }))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(scale)
                .publisher(TypedPublisher::new(MemoryPublish));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in 1..4u32 {
        tb.message(&Order { id })
            .to("scale")
            .publish()
            .await
            .expect("publish failed");
    }

    // Each id was multiplied by the state's multiplier (10), proving the handler read typed state.
    let scaled: Vec<Order> = tb
        .broker::<MemoryBroker>()
        .published::<Order>("scaled")
        .decoded();
    assert_eq!(
        scaled.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![10, 20, 30],
    );
}
