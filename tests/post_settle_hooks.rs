//! Integration tests for post-settle hooks (`ctx.after(..).then(..)`, `after_ack`,
//! `after_settle`) on the single-message and batch dispatch paths, driven through the
//! `#[subscriber]` macro over `MemoryBroker`.
//!
//! A hook runs off the delivery path, so the harness drains the continuations
//! ([`TestApp::drain`], or the drain a shutdown performs) before the counters are read.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use common::Order;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, SubscriberSettings};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{nonzero, subscriber};

/// What the hooks under test count, held in application state so a macro handler (a free fn)
/// reaches it the way a service reaches any dependency.
#[derive(Clone, Default)]
struct Counters {
    ack: Arc<AtomicU32>,
    dropped: Arc<AtomicU32>,
    retried: Arc<AtomicU32>,
    settle: Arc<AtomicU32>,
    handled: Arc<AtomicU32>,
}

impl Counters {
    fn read(counter: &AtomicU32) -> u32 {
        counter.load(Ordering::SeqCst)
    }
}

/// Odd ids ack, even ids drop (never retry); each registers an ack-gated, a drop-gated, a
/// retry-gated, and an ungated hook. The retry-gated one must never fire, proving drop and retry
/// are distinct mechanics.
#[subscriber("orders")]
async fn handle_order(order: &Order, ctx: &mut Context<'_, (), Counters>) -> HandlerOutcome {
    let c = ctx.state().clone();
    let outcome = if order.id % 2 == 1 {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    };

    let ack = Arc::clone(&c.ack);
    ctx.after(HandlerOutcome::ack()).then(async move {
        ack.fetch_add(1, Ordering::SeqCst);
    });
    let dropped = Arc::clone(&c.dropped);
    ctx.after(HandlerOutcome::drop()).then(async move {
        dropped.fetch_add(1, Ordering::SeqCst);
    });
    let retried = Arc::clone(&c.retried);
    ctx.after(HandlerOutcome::retry()).then(async move {
        retried.fetch_add(1, Ordering::SeqCst);
    });
    let settle = Arc::clone(&c.settle);
    ctx.after_settle(async move {
        settle.fetch_add(1, Ordering::SeqCst);
    });

    c.handled.fetch_add(1, Ordering::SeqCst);
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outcome_gated_and_ungated_hooks_fire_per_settlement() {
    let counters = Counters::default();
    let startup_counters = counters.clone();

    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(startup_counters) })
        .with_broker(MemoryBroker::new(), |b| b.include(handle_order));
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in [1u32, 2] {
        tb.message(&Order { id })
            .to("orders")
            .publish()
            .await
            .expect("publish");
    }

    assert_eq!(
        tb.broker::<MemoryBroker>().subscriber("orders").outcomes(),
        [Outcome::Ack, Outcome::Drop],
    );
    tb.drain().await;

    // One acked and one dropped delivery: one hook of each gate, and the ungated hook for both.
    assert_eq!(Counters::read(&counters.handled), 2);
    assert_eq!(Counters::read(&counters.ack), 1);
    assert_eq!(Counters::read(&counters.dropped), 1);
    assert_eq!(Counters::read(&counters.settle), 2);
    // Nothing ever retried, so the retry-gated hook never fired: drop does not trigger a retry hook.
    assert_eq!(
        Counters::read(&counters.retried),
        0,
        "a retry-gated hook must not fire when messages are dropped",
    );
}

/// A handler whose after-ack hook yields before completing, to prove graceful shutdown drains it.
#[subscriber("slow")]
async fn handle_slow(_order: &Order, ctx: &mut Context<'_, (), Counters>) -> HandlerOutcome {
    let done = Arc::clone(&ctx.state().ack);
    ctx.after_ack(async move {
        tokio::task::yield_now().await;
        done.fetch_add(1, Ordering::SeqCst);
    });
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_drain_on_graceful_shutdown() {
    let counters = Counters::default();
    let startup_counters = counters.clone();

    let app = RustStream::new(AppInfo::new("slow", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(startup_counters) })
        .with_broker(MemoryBroker::new(), |b| b.include(handle_slow));
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("slow")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("slow")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    // Shut down without draining first: the in-flight hook must still be drained by the shutdown.
    tb.shutdown().await.expect("graceful shutdown failed");
    assert_eq!(Counters::read(&counters.ack), 1, "hook was not drained");
}

/// A batch handler: the ungated after_settle hook fires once per batch; the outcome-gated one is
/// dropped on the batch path (per-element outcomes make a single gate ill-defined).
#[subscriber("batched")]
async fn handle_batch(orders: &[Order], ctx: &mut Context<'_, (), Counters>) -> HandlerOutcome {
    let _ = orders.len();
    let c = ctx.state().clone();
    let settle = Arc::clone(&c.settle);
    ctx.after_settle(async move {
        settle.fetch_add(1, Ordering::SeqCst);
    });
    let gated = Arc::clone(&c.ack);
    ctx.after(HandlerOutcome::ack()).then(async move {
        gated.fetch_add(1, Ordering::SeqCst);
    });
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_runs_after_settle_drops_outcome_gated() {
    let counters = Counters::default();
    let startup_counters = counters.clone();

    let app = RustStream::new(AppInfo::new("batched", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(startup_counters) })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle_batch.batch(nonzero!(64)));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    for id in 0..3u32 {
        tb.message(&Order { id })
            .to("batched")
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .subscriber("batched")
        .assert_called(3)
        .settled(HandlerOutcome::ack());
    tb.drain().await;

    // The ungated hook fired once per batch, and the outcome-gated one never did.
    assert_eq!(Counters::read(&counters.settle), 3);
    assert_eq!(
        Counters::read(&counters.ack),
        0,
        "outcome-gated hooks must not run on the batch path",
    );
}
