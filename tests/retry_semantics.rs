//! Integration tests for retry semantics at the dispatcher level: the `retry_after` delay is
//! honored (not merely "redelivery happens"), retries complete inside worker pools and keyed
//! lanes, and batch pools genuinely overlap batches.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::Order;
use futures::future::join_all;
use ruststream::HeaderMap;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::subscriber;
use ruststream::testing::{Outcome, TestApp};
use tokio::sync::{Notify, watch};

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The ids this subscription has already seen once. Held in application state so the handler
/// reads it the way a service reads any dependency.
#[derive(Default)]
struct FirstSeen(Mutex<Vec<u32>>);

impl FirstSeen {
    /// Records `id` and reports whether this is its first sighting.
    fn first(&self, id: u32) -> bool {
        let mut seen = self.0.lock().expect("the test holds no poisoned lock");
        if seen.contains(&id) {
            false
        } else {
            seen.push(id);
            true
        }
    }
}

/// Defers the first attempt by `RETRY_DELAY`, then acks.
#[subscriber("delayed")]
async fn deferred(order: &Order, ctx: &mut Context<'_, (), Arc<FirstSeen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// The dispatcher must hold the redelivery back for the full `retry_after` delay, measured on
/// the paused clock - not merely redeliver eventually.
///
/// `start_paused` requires the current-thread runtime (tokio cannot pause a multithreaded
/// clock); advancing the clock in two steps is what pins the delay: the redelivery is still
/// absent one tick short of it, and lands on the tick that reaches it.
#[tokio::test(start_paused = true)]
async fn retry_after_delay_is_honored_by_the_dispatcher() {
    let app = RustStream::new(AppInfo::new("delayed", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, std::convert::Infallible>(Arc::new(FirstSeen::default()))
        })
        .with_broker(MemoryBroker::new(), |b| b.include(deferred));
    let tb = TestApp::start(app).await.expect("startup failed");

    // One publish is enough: the second attempt must come from the delayed redelivery.
    tb.message(&Order { id: 1 })
        .to("delayed")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // One tick short of the delay the message is still held back.
    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called_once();

    // The tick that reaches the delay releases it.
    tb.advance(Duration::from_millis(1)).await.expect("settle");
    assert_eq!(
        tb.broker::<MemoryBroker>().subscriber("delayed").outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
}

/// First sight of each id asks for an immediate retry; the redelivery is acked.
#[subscriber("pool-retry", workers(3))]
async fn pool_retry(order: &Order, ctx: &mut Context<'_, (), Arc<FirstSeen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

/// Retried deliveries re-enter a worker pool and complete: every message is nacked once
/// (requeue) and acked on the second pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_completes_inside_a_worker_pool() {
    let app = RustStream::new(AppInfo::new("pool-retry", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, std::convert::Infallible>(Arc::new(FirstSeen::default()))
        })
        .with_broker(MemoryBroker::new(), |b| b.include(pool_retry));
    let tb = TestApp::start(app).await.expect("startup failed");

    // Injected together, so the pool has several deliveries to spread over its workers; the
    // publishes all resolve once the whole reaction, redeliveries included, has settled.
    let orders: Vec<Order> = (1..=4u32).map(|id| Order { id }).collect();
    for result in join_all(
        orders
            .iter()
            .map(|order| tb.message(order).to("pool-retry").publish()),
    )
    .await
    {
        result.expect("publish");
    }

    // 4 distinct ids, each retried once then acked. The pool decides the interleaving, so the
    // counts are what is promised, not the order.
    let outcomes = tb
        .broker::<MemoryBroker>()
        .subscriber("pool-retry")
        .outcomes();
    assert_eq!(outcomes.iter().filter(|o| **o == Outcome::Nack).count(), 4);
    assert_eq!(outcomes.iter().filter(|o| **o == Outcome::Ack).count(), 4);
    tb.broker::<MemoryBroker>()
        .subscriber("pool-retry")
        .assert_called(8);
}

/// First sight of each id asks for an immediate retry; the redelivery is acked.
#[subscriber("lane-retry", workers(2, by_key))]
async fn lane_retry(order: &Order, ctx: &mut Context<'_, (), Arc<FirstSeen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

/// Retried deliveries re-enter keyed lanes and complete (per-key ordering across a retry is
/// not promised: a requeued message rejoins the stream from the back).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_completes_inside_keyed_lanes() {
    let app = RustStream::new(AppInfo::new("lane-retry", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, std::convert::Infallible>(Arc::new(FirstSeen::default()))
        })
        .with_broker(MemoryBroker::new(), |b| b.include(lane_retry));
    let tb = TestApp::start(app).await.expect("startup failed");

    let keyed = |key: &'static str, id: u32| {
        let mut headers = HeaderMap::new();
        headers.insert("partition-key", key);
        (Order { id }, headers)
    };
    let inputs = [keyed("alpha", 1), keyed("beta", 2)];
    // Two keyed messages, each retried once then acked, injected together so both lanes are live.
    for result in join_all(inputs.iter().map(|(order, headers)| {
        tb.message(order)
            .with_headers(headers.clone())
            .to("lane-retry")
            .publish()
    }))
    .await
    {
        result.expect("publish");
    }

    let outcomes = tb
        .broker::<MemoryBroker>()
        .subscriber("lane-retry")
        .outcomes();
    assert_eq!(outcomes.iter().filter(|o| **o == Outcome::Nack).count(), 2);
    assert_eq!(outcomes.iter().filter(|o| **o == Outcome::Ack).count(), 2);
    tb.broker::<MemoryBroker>()
        .subscriber("lane-retry")
        .assert_called(4);
}

// The subject below IS the dispatcher's batch pool: it is observed mid-reaction, with one batch
// deliberately held while a second is pulled, which the harness's drive-to-quiescence publish
// cannot express - so this one keeps the running app and its own latches.

static BATCHES_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static OVERLAP_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
/// Flipping this to `true` releases every held and future batch, so no handler task can outlive
/// the test and hang the graceful drain (a barrier would strand a third, unpaired batch).
static RELEASE: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));

/// Holds every batch until the test observes two of them in flight at once.
#[subscriber("overlap", workers(2))]
async fn overlap(_orders: &[Order]) -> HandlerOutcome {
    BATCHES_IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    OVERLAP_NOTIFY.notify_one();
    let mut release = RELEASE.subscribe();
    let _ = release.wait_for(|released| *released).await;
    BATCHES_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

/// A batch pool genuinely overlaps batches: with `workers(2)`, a second batch is pulled and
/// dispatched while the first is still being handled (both sit on the release latch at once).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_overlaps_batches() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("overlap", "0.1.0"))
        .with_broker(broker, |b| b.include(overlap));

    let running = app.start().await.expect("startup failed");

    // Publish the second message only after the first batch is held on the latch, so the two
    // messages arrive in distinct batches - which a sequential batch loop could never hold in
    // flight simultaneously.
    publisher
        .message(&Order { id: 1 })
        .to("overlap")
        .publish()
        .await
        .expect("publish");
    let first_held = tokio::time::timeout(Duration::from_secs(5), async {
        while BATCHES_IN_FLIGHT.load(Ordering::SeqCst) < 1 {
            OVERLAP_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(first_held.is_ok(), "the first batch never reached the pool");

    publisher
        .message(&Order { id: 2 })
        .to("overlap")
        .publish()
        .await
        .expect("publish");
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while BATCHES_IN_FLIGHT.load(Ordering::SeqCst) < 2 {
            OVERLAP_NOTIFY.notified().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "two batches were never in flight at once through the pool",
    );

    // Release every held (and any late) batch so the graceful drain can finish.
    RELEASE.send_replace(true);
    running.shutdown().await.expect("graceful shutdown failed");
}
