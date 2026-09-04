//! A batch body that answers with one outcome for the whole batch.
//!
//! An acking batch with nothing attached is the fast path: the batch settles as a unit. Every other
//! uniform answer - a refusal, or an ack carrying post-settle work - fans out to a per-element
//! settlement, and the attached work rides the last element so a batch runs it at most once.
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
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, SubscriberSettings};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{nonzero, subscriber};

/// Refuses the whole batch at once: one outcome answers for every element in it.
#[subscriber("uniform-drop")]
async fn refuse(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::drop()
}

/// Every element of a refused batch is settled by that one outcome, so nothing is left unsettled
/// behind an answer that named no element in particular.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uniform_refusal_settles_every_element_of_the_batch() {
    let app =
        RustStream::new(AppInfo::new("uniform", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(refuse.batch(nonzero!(64)));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in 0..3u32 {
        tb.message(&Order { id })
            .to("uniform-drop")
            .publish()
            .await
            .expect("publish failed");
    }

    // Exactly the three published ids: a drop is a refusal, not a redelivery, so no element
    // comes back for a second run either.
    let refused: Vec<Order> = tb
        .broker::<MemoryBroker>()
        .subscriber("uniform-drop")
        .received();
    assert_eq!(refused.iter().map(|o| o.id).collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("uniform-drop")
            .outcomes(),
        [Outcome::Drop, Outcome::Drop, Outcome::Drop],
    );
    tb.broker::<MemoryBroker>()
        .subscriber("uniform-drop")
        .assert_called(3)
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

/// One message makes one batch, so the attached work runs exactly once - the assertion that pins
/// "once per batch" rather than "once per element".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uniform_ack_runs_its_attached_work_once_for_the_batch() {
    let continued = Arc::new(AtomicUsize::new(0));
    let state_counter = Arc::clone(&continued);
    let app = RustStream::new(AppInfo::new("uniform-after", "0.1.0"))
        .on_startup(move |()| {
            let counter = state_counter;
            async move { Ok::<_, std::convert::Infallible>(Continued(counter)) }
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(accept.batch(nonzero!(64)));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 9 })
        .to("uniform-after")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber("uniform-after")
        .assert_called_once()
        .with(&Order { id: 9 })
        .settled(HandlerOutcome::ack());
    // The continuation runs off the delivery path, so the harness drains it before the count is
    // read.
    tb.drain().await;
    assert_eq!(continued.load(Ordering::SeqCst), 1);
}
