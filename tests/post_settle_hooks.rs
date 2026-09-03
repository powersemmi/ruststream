//! Integration tests for post-settle hooks (`ctx.after(..).then(..)`, `after_ack`,
//! `after_settle`) on the single-message and batch dispatch paths, driven through the
//! `#[subscriber]` macro over `MemoryBroker`.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use common::{BackgroundRun, Order, order_bytes, wait_for};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::subscriber;
use tokio::sync::Notify;

// Shared counters keyed by a static so the macro handler (a free fn) can reach them.
#[derive(Clone, Default)]
struct Counters {
    ack: Arc<AtomicU32>,
    dropped: Arc<AtomicU32>,
    retried: Arc<AtomicU32>,
    settle: Arc<AtomicU32>,
    handled: Arc<AtomicU32>,
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
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let counters = Counters::default();
    let startup_counters = counters.clone();

    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(startup_counters) })
        .with_broker(broker, |b| b.include(handle_order));

    let run = BackgroundRun::spawn(app);

    // The macro subscription opens inside run(); retry until both deliveries land.
    let publish = |id: u32| {
        let publisher = &publisher;
        async move {
            let _ = publisher.raw(&order_bytes(id)).to("orders").publish().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            publish(1).await;
            publish(2).await;
            if counters.handled.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deliveries within deadline");

    // One ack-gated, one drop-gated; both settle hooks ran. Use >= since the retry loop may
    // publish extra copies before the first pair is handled.
    wait_for(
        || {
            counters.ack.load(Ordering::SeqCst) >= 1
                && counters.dropped.load(Ordering::SeqCst) >= 1
                && counters.settle.load(Ordering::SeqCst) >= 2
        },
        Duration::from_secs(5),
    )
    .await;

    // Nothing ever retried, so the retry-gated hook never fired: drop does not trigger a retry hook.
    assert_eq!(
        counters.retried.load(Ordering::SeqCst),
        0,
        "a retry-gated hook must not fire when messages are dropped",
    );

    run.stop().await;
}

static SLOW_DONE: AtomicU32 = AtomicU32::new(0);
static SLOW_HANDLED: Notify = Notify::const_new();

/// A handler whose after-ack hook yields before completing, to prove graceful shutdown drains it.
#[subscriber("slow")]
async fn handle_slow(_order: &Order, ctx: &mut Context) -> HandlerOutcome {
    ctx.after_ack(async {
        tokio::task::yield_now().await;
        SLOW_DONE.fetch_add(1, Ordering::SeqCst);
    });
    SLOW_HANDLED.notify_one();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_drain_on_graceful_shutdown() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("slow", "0.1.0"))
        .with_broker(broker, |b| b.include(handle_slow));

    let run = BackgroundRun::spawn(app);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher.raw(&order_bytes(1)).to("slow").publish().await;
            tokio::select! {
                () = SLOW_HANDLED.notified() => break,
                () = tokio::task::yield_now() => {}
            }
        }
    })
    .await
    .expect("delivery within deadline");

    // Request shutdown immediately; the in-flight hook must still be drained.
    run.stop().await;
    assert!(
        SLOW_DONE.load(Ordering::SeqCst) >= 1,
        "hook was not drained"
    );
}

static BATCH_SETTLE: AtomicU32 = AtomicU32::new(0);
static BATCH_GATED: AtomicU32 = AtomicU32::new(0);
static BATCH_NOTIFY: Notify = Notify::const_new();

/// A batch handler: the ungated after_settle hook fires once per batch; the outcome-gated one is
/// dropped on the batch path (per-element outcomes make a single gate ill-defined).
#[subscriber(batch("batched"))]
async fn handle_batch(orders: &[Order], ctx: &mut Context) -> HandlerOutcome {
    let _ = orders.len();
    ctx.after_settle(async {
        BATCH_SETTLE.fetch_add(1, Ordering::SeqCst);
        BATCH_NOTIFY.notify_one();
    });
    ctx.after(HandlerOutcome::ack()).then(async {
        BATCH_GATED.fetch_add(1, Ordering::SeqCst);
    });
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_runs_after_settle_drops_outcome_gated() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("batched", "0.1.0"))
        .with_broker(broker, |b| b.include(handle_batch));

    let run = BackgroundRun::spawn(app);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for id in 0..3u32 {
                let _ = publisher
                    .raw(&order_bytes(id))
                    .to("batched")
                    .publish()
                    .await;
            }
            tokio::select! {
                () = BATCH_NOTIFY.notified() => break,
                () = tokio::task::yield_now() => {}
            }
            if BATCH_SETTLE.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await
    .expect("batch settled within deadline");

    // Let any (incorrect) outcome-gated hook run before asserting it did not.
    tokio::task::yield_now().await;
    assert_eq!(
        BATCH_GATED.load(Ordering::SeqCst),
        0,
        "outcome-gated hooks must not run on the batch path",
    );

    run.stop().await;
}
