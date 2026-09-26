//! Integration tests for the workers(..) dispatch policies: concurrent pools and per-key lanes.
//!
//! The pool tests inject their deliveries together rather than one at a time: a pool only has
//! something to spread over its workers while more than one delivery is in flight, and the
//! harness settles the whole reaction before the injections resolve.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use common::Order;
use futures::future::join_all;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use tokio::sync::Barrier;

/// The deadline every "did the pool run these together?" wait rides. A pool that dispatched
/// sequentially would park on the barrier forever, so the timeout is what turns that deadlock
/// into a failed assertion.
const CONCURRENCY_DEADLINE: Duration = Duration::from_secs(5);

/// Four deliveries must be in flight at once to pass the barrier; a sequential loop would
/// deadlock on the first one.
#[subscriber("jobs", workers(4))]
async fn crunch(_job: &Order, ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_processes_deliveries_concurrently() {
    let app = RustStream::new(AppInfo::new("jobs", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Arc::new(Barrier::new(4))))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(crunch);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    // Exactly the barrier's worth of jobs: dispatched sequentially, the first would park on the
    // barrier and the deadline below would expire.
    let jobs: Vec<Order> = (1..=4u32).map(|id| Order { id }).collect();
    let published = tokio::time::timeout(
        CONCURRENCY_DEADLINE,
        join_all(jobs.iter().map(|job| tb.message(job).to("jobs").publish())),
    )
    .await
    .expect("the pool must hold four deliveries in flight at once");
    for result in published {
        result.expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .subscriber("jobs")
        .assert_called(4)
        .settled(HandlerOutcome::ack());
}

/// Records nothing of its own: the id carries the key, so the harness's delivery order per key
/// is what the assertion reads.
#[subscriber("keyed", workers(4, by_key))]
async fn keyed(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    // Encourage interleaving between lanes; each lane itself stays sequential.
    tokio::task::yield_now().await;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn by_key_lanes_preserve_per_key_order() {
    const PER_KEY: u32 = 10;
    // The key rides the headers (that is what picks the lane) and the id band says which key a
    // delivery belongs to, so per-key order is readable off the recorded deliveries alone.
    const BETA_BAND: u32 = 100;

    let app =
        RustStream::new(AppInfo::new("keyed", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(keyed);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    let keyed_input = |key: &'static str, id: u32| {
        let mut headers = HeaderMap::new();
        headers.insert("partition-key", key);
        (Order { id }, headers)
    };
    let inputs: Vec<_> = (1..=PER_KEY)
        .flat_map(|id| {
            [
                keyed_input("alpha", id),
                keyed_input("beta", id + BETA_BAND),
            ]
        })
        .collect();

    // Injected together, in publish order, so the lanes have a stream to keep in order.
    for result in join_all(inputs.iter().map(|(order, headers)| {
        tb.message(order)
            .with_headers(headers.clone())
            .to("keyed")
            .publish()
    }))
    .await
    {
        result.expect("publish");
    }

    let seen: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("keyed").received();
    for band in [0, BETA_BAND] {
        let ids: Vec<u32> = seen
            .iter()
            .map(|order| order.id)
            .filter(|id| (*id > band) && (*id <= band + PER_KEY))
            .collect();
        assert_eq!(
            ids.len(),
            PER_KEY as usize,
            "the whole key band must arrive: {ids:?}",
        );
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "per-key order lost in band {band}: {ids:?}",
        );
    }
}

/// How long the deferring handlers below ask to wait.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// The orders a handler has already seen once: application state, the way a service shares
/// anything with its handlers.
#[derive(Default)]
struct Seen {
    orders: Mutex<Vec<u32>>,
}

impl Seen {
    /// Records `id` and reports whether this is its first sighting.
    fn first(&self, id: u32) -> bool {
        let mut orders = self.orders.lock().expect("unpoisoned");
        if orders.contains(&id) {
            false
        } else {
            orders.push(id);
            true
        }
    }
}

/// Defers the first sight of every order, acks its redelivery.
#[subscriber("deferred", workers(4))]
async fn defer_pooled(order: &Order, ctx: &mut Context<'_, (), Arc<Seen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// The same over keyed lanes.
#[subscriber("deferred", workers(4, by_key))]
async fn defer_laned(order: &Order, ctx: &mut Context<'_, (), Arc<Seen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Publishes two orders to a deferring pool on a paused clock and walks the clock to the delay:
/// the workers run on the test's runtime, so the redeliveries they arm wait for `advance` and
/// land on the tick that reaches the delay.
async fn deferred_redeliveries_follow_the_harness_clock(tb: &TestApp<Arc<Seen>>) {
    let orders = [Order { id: 1 }, Order { id: 2 }];
    for result in join_all(
        orders
            .iter()
            .map(|order| tb.message(order).to("deferred").publish()),
    )
    .await
    {
        result.expect("publish");
    }
    tb.broker::<MemoryBroker>()
        .subscriber("deferred")
        .assert_called(2);

    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("deferred")
        .assert_called(2);

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    let mut outcomes = tb
        .broker::<MemoryBroker>()
        .subscriber("deferred")
        .outcomes();
    outcomes.sort_by_key(|outcome| *outcome == Outcome::Ack);
    assert_eq!(
        outcomes,
        [Outcome::Nack, Outcome::Nack, Outcome::Ack, Outcome::Ack],
        "both redeliveries must land on the tick that reaches the delay",
    );
}

#[tokio::test(start_paused = true)]
async fn a_pool_arms_its_redeliveries_on_the_harness_clock() {
    let app = RustStream::new(AppInfo::new("deferred", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Arc::new(Seen::default())))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(defer_pooled);
        });
    let tb = TestApp::start(app).await.expect("startup failed");
    deferred_redeliveries_follow_the_harness_clock(&tb).await;
}

#[tokio::test(start_paused = true)]
async fn keyed_lanes_arm_their_redeliveries_on_the_harness_clock() {
    let app = RustStream::new(AppInfo::new("deferred", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Arc::new(Seen::default())))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(defer_laned);
        });
    let tb = TestApp::start(app).await.expect("startup failed");
    deferred_redeliveries_follow_the_harness_clock(&tb).await;
}
