//! A batch body that answers with one outcome for the whole batch.
//!
//! An acking batch with nothing attached is the fast path: the batch settles as a unit. Every other
//! uniform answer - a refusal, or an ack carrying post-settle work - fans out to a per-element
//! settlement, and the attached work rides the last element so a batch runs it at most once.
//!
//! The harness settles each injection before the next, which would hand a body batches of one;
//! every service here opens its subscription at the start of a log published beforehand instead,
//! so the opening replay hands the body the whole run as one batch.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::Order;
use ruststream::memory::{MemoryBroker, MemoryPosition, Retaining, Retention};
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream, SubscriberSettings};
use ruststream::testing::TestApp;
use ruststream::{nonzero, subscriber};

/// A broker holding three orders on `subject` before any subscription exists.
async fn three_orders_on(subject: &str) -> MemoryBroker<Retaining> {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(32)));
    let publisher = broker.publisher();
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to(subject)
            .publish()
            .await
            .expect("publish failed");
    }
    broker
}

/// Refuses the whole batch at once: one outcome answers for every element in it.
#[subscriber("uniform-drop")]
async fn refuse(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::drop()
}

/// Every element of a refused batch is settled by that one outcome, so nothing is left unsettled
/// behind an answer that named no element in particular, and nothing comes back for a second run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uniform_refusal_settles_every_element_of_the_batch() {
    let broker = three_orders_on("uniform-drop").await;
    let app = RustStream::new(AppInfo::new("uniform", "0.1.0")).with_broker(broker, |b| {
        b.include(refuse.batch(nonzero!(8)).start_at(MemoryPosition::start()));
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batch settles");

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("uniform-drop")
        .assert_batch_sizes(&[3])
        .settled(HandlerOutcome::drop());
}

/// How many times the batch's attached post-settle work ran. It lives in application state, which
/// is what a continuation writing to a dependency looks like in a service.
struct Continued(Arc<AtomicUsize>);

/// Acks the whole batch and attaches one piece of post-settle work to that single answer.
#[subscriber("uniform-after")]
async fn accept(orders: &[Order], ctx: &mut Context<'_, (), Continued>) -> HandlerOutcome {
    let _ = orders;
    let continued = Arc::clone(&ctx.state().0);
    HandlerOutcome::ack().and_after(async move {
        continued.fetch_add(1, Ordering::SeqCst);
    })
}

/// The ack carrying work leaves the fast path: every element is acked, and the work survives the
/// fan-out to run once for the batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uniform_ack_runs_its_attached_work_once_for_the_batch() {
    let continued = Arc::new(AtomicUsize::new(0));
    let state_counter = Arc::clone(&continued);
    let broker = three_orders_on("uniform-after").await;
    let app = RustStream::new(AppInfo::new("uniform-after", "0.1.0"))
        .on_startup(move |()| {
            let counter = state_counter;
            async move { Ok::<_, std::convert::Infallible>(Continued(counter)) }
        })
        .with_broker(broker, |b| {
            b.include(accept.batch(nonzero!(8)).start_at(MemoryPosition::start()));
        });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batch settles");

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("uniform-after")
        .assert_batch_sizes(&[3])
        .settled(HandlerOutcome::ack());
    // The continuation runs off the delivery path, so the harness drains it before the count is
    // read.
    tb.drain().await;
    assert_eq!(continued.load(Ordering::SeqCst), 1);
}
