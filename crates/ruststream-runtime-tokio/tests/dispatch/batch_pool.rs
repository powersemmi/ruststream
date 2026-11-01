//! A pooled batch subscription (`.batch(n)` under `workers(n)`) runs on the crew it started with:
//! every delivery settles once, batches overlap, and a batch costs no task of its own.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::join_all;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use tokio::time::timeout;

/// A pool that ran its batches one after another would park on the barrier until this expires.
const OVERLAP_DEADLINE: Duration = Duration::from_secs(5);

/// The message the batches carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
struct Job {
    id: u32,
}

/// Counts the runtime's live tasks seen from inside the batches.
#[derive(Clone, Default, FromRef)]
struct Census {
    alive: Arc<Mutex<Vec<usize>>>,
}

/// Two batches must be in flight together to pass the barrier.
#[subscriber("overlap", workers(2))]
async fn overlap(_jobs: &[Job], ctx: &mut Context<'_, (), Arc<Barrier>>) -> HandlerOutcome {
    ctx.state().wait().await;
    HandlerOutcome::ack()
}

/// Records how many tasks the runtime holds while the batch runs.
#[subscriber("census", workers(2))]
async fn census(_jobs: &[Job], ctx: &mut Context<'_, (), Census>) -> HandlerOutcome {
    let alive = Handle::current().metrics().num_alive_tasks();
    ctx.state().alive.lock().expect("census").push(alive);
    HandlerOutcome::ack()
}

/// Settles every delivery.
#[subscriber("plain", workers(2))]
async fn plain(_jobs: &[Job]) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_hands_every_delivery_to_a_batch_once() {
    let app = RustStream::new(AppInfo::new("plain", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(plain.batch(nonzero!(2))));
    let tb = TestApp::start(app).await.expect("startup failed");

    let jobs: Vec<Job> = (1..=8u32).map(|id| Job { id }).collect();
    let published = join_all(jobs.iter().map(|job| tb.message(job).to("plain").publish())).await;
    for result in published {
        result.expect("publish");
    }
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .subscriber("plain")
        .assert_called(4)
        .assert_batch_sizes(&[2, 2, 2, 2])
        .settled(HandlerOutcome::ack());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_overlap_on_the_pool() {
    let app = RustStream::new(AppInfo::new("overlap", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Barrier::new(2))))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(overlap.batch(nonzero!(1)))
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    let jobs: Vec<Job> = (1..=2u32).map(|id| Job { id }).collect();
    let published = timeout(
        OVERLAP_DEADLINE,
        join_all(
            jobs.iter()
                .map(|job| tb.message(job).to("overlap").publish()),
        ),
    )
    .await
    .expect("the pool must hold two batches in flight at once");
    for result in published {
        result.expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .subscriber("overlap")
        .assert_batch_sizes(&[1, 1])
        .settled(HandlerOutcome::ack());
}

/// The crew is started with the subscription, so a batch runs on a task that was already there:
/// the runtime holds the same number of tasks while a batch runs as before any arrived.
#[tokio::test]
async fn a_batch_costs_no_task_of_its_own() {
    let seen = Census::default();
    let app = RustStream::new(AppInfo::new("census", "0.1.0"))
        .on_startup({
            let seen = seen.clone();
            async move |()| Ok::<_, Infallible>(seen.clone())
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(census.batch(nonzero!(1)))
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    // The first batch has the loop start its crew; what the runtime holds once it settled is
    // what every later batch must find.
    let mut before = 0;
    for id in 0..=3u32 {
        tb.message(&Job { id })
            .to("census")
            .publish()
            .await
            .expect("publish");
        tb.settle().await.expect("settle");
        if id == 0 {
            before = Handle::current().metrics().num_alive_tasks();
        }
    }

    let alive = seen.alive.lock().expect("census");
    assert_eq!(alive[1..], [before; 3]);
}
