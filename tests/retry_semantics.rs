//! Integration tests for retry semantics at the dispatcher level: a zero `retry_after` delay needs
//! no clock on a broker that holds the delivery back itself, and batch pools genuinely overlap
//! batches.
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
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, SubscriberSettings};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{nonzero, subscriber};
use tokio::sync::Barrier;

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

/// Defers the first attempt by no time at all, then acks.
#[subscriber("undelayed")]
async fn undeferred(order: &Order, ctx: &mut Context<'_, (), Arc<FirstSeen>>) -> HandlerOutcome {
    if ctx.state().first(order.id) {
        HandlerOutcome::retry_after(Duration::ZERO)
    } else {
        HandlerOutcome::ack()
    }
}

/// A zero delay is no delay: the in-memory broker puts the delivery back at once, so the
/// redelivery lands in the reaction of the publish itself and no `advance` is needed. The runtime
/// has worker threads, so a redelivery left to a timer task would still be on its way when the
/// publish returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_retry_after_delay_redelivers_without_advancing_the_clock() {
    let app = RustStream::new(AppInfo::new("undelayed", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, std::convert::Infallible>(Arc::new(FirstSeen::default()))
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(undeferred);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("undelayed")
        .publish()
        .await
        .expect("publish");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("undelayed")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
}

/// A batch pool that ran its batches one at a time parks on the barrier forever; the deadline
/// turns that into a failed assertion.
const OVERLAP_DEADLINE: Duration = Duration::from_secs(5);

/// Passes the barrier only with two batches in flight at once.
#[subscriber("overlap", workers(2))]
async fn overlap(_orders: &[Order], ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    HandlerOutcome::ack()
}

/// A batch pool genuinely overlaps batches: with `workers(2)` and one message per batch, the
/// second batch is pulled and handled while the first is still being handled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_pool_overlaps_batches() {
    let app = RustStream::new(AppInfo::new("overlap", "0.1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(Arc::new(Barrier::new(2))))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(overlap.batch(nonzero!(1)));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    let orders = [Order { id: 1 }, Order { id: 2 }];
    let published = tokio::time::timeout(
        OVERLAP_DEADLINE,
        join_all(
            orders
                .iter()
                .map(|order| tb.message(order).to("overlap").publish()),
        ),
    )
    .await
    .expect("two batches were never in flight at once through the pool");
    for result in published {
        result.expect("publish");
    }
    tb.broker::<MemoryBroker>()
        .subscriber("overlap")
        .assert_batch_sizes(&[1, 1]);
}
