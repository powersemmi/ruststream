//! A batch body that answers with one outcome for the whole page.
//!
//! An acking page with nothing attached is the fast path: the page settles as a unit. Every other
//! uniform answer - a refusal, or an ack carrying post-settle work - fans out to a per-element
//! settlement, and the attached work rides the last element so a page runs it at most once.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Order, wait_for};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::subscriber;

static REFUSED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// Refuses the whole page at once: one outcome answers for every element in it.
#[subscriber("uniform-drop")]
async fn refuse(orders: &[Order]) -> HandlerOutcome {
    REFUSED
        .lock()
        .unwrap()
        .extend(orders.iter().map(|order| order.id));
    HandlerOutcome::drop()
}

/// Every element of a refused page is settled by that one outcome, so nothing is left unsettled
/// behind an answer that named no element in particular.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uniform_refusal_settles_every_element_of_the_page() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("uniform", "0.1.0"))
        .with_broker(broker, |b| b.include(refuse));
    let running = app.start().await.expect("startup failed");

    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("uniform-drop")
            .publish()
            .await
            .expect("publish failed");
    }
    wait_for(
        || REFUSED.lock().unwrap().len() >= 3,
        Duration::from_secs(5),
    )
    .await;

    // Exactly the three published ids: a drop is a refusal, not a redelivery, so no element
    // comes back for a second run either.
    let mut refused = REFUSED.lock().unwrap().clone();
    refused.sort_unstable();
    assert_eq!(refused, vec![0, 1, 2]);

    running.shutdown().await.expect("graceful shutdown failed");
}

static ACCEPTED: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static CONTINUED: AtomicUsize = AtomicUsize::new(0);

/// Acks the whole page and attaches one piece of post-settle work to that single answer.
#[subscriber("uniform-after")]
async fn accept(orders: &[Order]) -> HandlerOutcome {
    ACCEPTED
        .lock()
        .unwrap()
        .extend(orders.iter().map(|order| order.id));
    HandlerOutcome::ack().and_after(async move {
        CONTINUED.fetch_add(1, Ordering::SeqCst);
    })
}

/// One message makes one page, so the attached work runs exactly once - the assertion that pins
/// "once per page" rather than "once per element".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uniform_ack_runs_its_attached_work_once_for_the_page() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("uniform-after", "0.1.0"))
        .with_broker(broker, |b| b.include(accept));
    let running = app.start().await.expect("startup failed");

    publisher
        .message(&Order { id: 9 })
        .to("uniform-after")
        .publish()
        .await
        .expect("publish failed");
    wait_for(
        || CONTINUED.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(ACCEPTED.lock().unwrap().as_slice(), [9]);
    assert_eq!(CONTINUED.load(Ordering::SeqCst), 1);

    running.shutdown().await.expect("graceful shutdown failed");
}
