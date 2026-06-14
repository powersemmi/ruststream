//! Integration tests for post-settle hooks (`ctx.after(..).then(..)`, `after_ack`,
//! `after_settle`) on the single-message and batch dispatch paths, driven through the
//! `#[subscriber]` macro over `MemoryBroker`.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).unwrap()
}

async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(result.is_ok(), "condition not met within {timeout:?}");
}

// Shared counters keyed by a static so the macro handler (a free fn) can reach them.
#[derive(Clone, Default)]
struct Counters {
    ack: Arc<AtomicU32>,
    nack: Arc<AtomicU32>,
    settle: Arc<AtomicU32>,
    handled: Arc<AtomicU32>,
}

/// Odd ids ack, even ids drop; each registers an ack-gated, a nack-gated and an ungated hook.
#[subscriber("orders")]
async fn handle_order(order: &Order, ctx: &mut Context) -> HandlerResult {
    let c = ctx.get::<Counters>().expect("counters in state").clone();
    let outcome = if order.id % 2 == 1 {
        HandlerResult::Ack
    } else {
        HandlerResult::drop()
    };

    let ack = Arc::clone(&c.ack);
    ctx.after(HandlerResult::Ack).then(async move {
        ack.fetch_add(1, Ordering::SeqCst);
    });
    let nack = Arc::clone(&c.nack);
    ctx.after(HandlerResult::drop()).then(async move {
        nack.fetch_add(1, Ordering::SeqCst);
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

    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .insert_state(counters.clone())
        .with_broker(broker, |b| b.include(handle_order));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // The macro subscription opens inside run(); retry until both deliveries land.
    let publish = |id: u32| {
        let publisher = &publisher;
        async move {
            let _ = publisher
                .publish(OutgoingMessage::new("orders", &order_bytes(id)))
                .await;
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

    // One ack-gated, one nack-gated; both settle hooks ran. Use >= since the retry loop may
    // publish extra copies before the first pair is handled.
    wait_for(
        || {
            counters.ack.load(Ordering::SeqCst) >= 1
                && counters.nack.load(Ordering::SeqCst) >= 1
                && counters.settle.load(Ordering::SeqCst) >= 2
        },
        Duration::from_secs(5),
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

static SLOW_DONE: AtomicU32 = AtomicU32::new(0);
static SLOW_HANDLED: Notify = Notify::const_new();

/// A handler whose after-ack hook yields before completing, to prove graceful shutdown drains it.
#[subscriber("slow")]
async fn handle_slow(_order: &Order, ctx: &mut Context) -> HandlerResult {
    ctx.after_ack(async {
        tokio::task::yield_now().await;
        SLOW_DONE.fetch_add(1, Ordering::SeqCst);
    });
    SLOW_HANDLED.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_drain_on_graceful_shutdown() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("slow", "0.1.0"))
        .with_broker(broker, |b| b.include(handle_slow));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("slow", &order_bytes(1)))
                .await;
            tokio::select! {
                () = SLOW_HANDLED.notified() => break,
                () = tokio::task::yield_now() => {}
            }
        }
    })
    .await
    .expect("delivery within deadline");

    // Request shutdown immediately; the in-flight hook must still be drained.
    shutdown.notify_one();
    run.await.unwrap().unwrap();
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
async fn handle_batch(orders: &[Order], ctx: &mut Context) -> HandlerResult {
    let _ = orders.len();
    ctx.after_settle(async {
        BATCH_SETTLE.fetch_add(1, Ordering::SeqCst);
        BATCH_NOTIFY.notify_one();
    });
    ctx.after(HandlerResult::Ack).then(async {
        BATCH_GATED.fetch_add(1, Ordering::SeqCst);
    });
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_runs_after_settle_drops_outcome_gated() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("batched", "0.1.0"))
        .with_broker(broker, |b| b.include_batch(handle_batch));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for id in 0..3u32 {
                let _ = publisher
                    .publish(OutgoingMessage::new("batched", &order_bytes(id)))
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

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
