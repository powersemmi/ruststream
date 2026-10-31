//! A subscription whose deliveries never wait must not starve the executor: the app's one task
//! yields between deliveries, so another task of the same executor still runs.
//!
//! One handler retries every delivery at once, forever. The driver task, beside the app on the
//! same executor, publishes to the spinning subscription and then to a second one: the second
//! handler must see its message while the first still spins.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicU32, Ordering};

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, SubscriberSettings};
use ruststream::{Broker, Deserialized, LocalPublisher, OutgoingMessage, subscriber};
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A message as its bytes.
#[derive(Deserialized)]
struct Raw<'a>(&'a [u8]);

/// How often the spinning handler ran.
static SPINS: AtomicU32 = AtomicU32::new(0);

/// How often the second handler ran.
static CALMS: AtomicU32 = AtomicU32::new(0);

/// Asks for an immediate retry on every delivery, so the same message comes back at once.
#[subscriber(LocalMemorySource::new("bus/spin"))]
async fn spin(raw: &Raw<'_>) -> HandlerOutcome {
    let _ = raw.0;
    SPINS.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::retry()
}

/// Acknowledges what it receives.
#[subscriber(LocalMemorySource::new("bus/calm"))]
async fn calm(raw: &Raw<'_>) -> HandlerOutcome {
    let _ = raw.0;
    CALMS.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::ack()
}

/// Asks for an immediate retry on every delivery, under a mount with room for retry copies.
#[subscriber(LocalMemorySource::new("bus/kept"))]
async fn kept(raw: &Raw<'_>) -> HandlerOutcome {
    let _ = raw.0;
    KEPT.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::retry()
}

/// How often the handler with room for retry copies ran.
static KEPT: AtomicU32 = AtomicU32::new(0);

/// Asks for an immediate retry of every batch, so the same readings come back at once.
#[subscriber(LocalMemorySource::new("bus/batch"))]
async fn batch(readings: &[Raw<'_>]) -> HandlerOutcome {
    let _ = readings.len();
    BATCHES.fetch_add(1, Ordering::Relaxed);
    HandlerOutcome::retry()
}

/// How often the batch handler ran.
static BATCHES: AtomicU32 = AtomicU32::new(0);

/// The size the batch subscription reads its batches at.
const BATCH: NonZeroUsize = NonZeroUsize::new(2).expect("two is not zero");

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(spin)
            .include(kept)
            .retry_capacity::<1>()
            .include(calm)
            .include(batch.batch(BATCH))
    })
}

/// What the scenario keeps in statics.
struct Stand {
    bus: Bus,
    done: Cell<bool>,
}

/// Lets every task the last step woke run until it parks again.
async fn settle() {
    for _ in 0..SETTLE_ROUNDS {
        yield_now().await;
    }
}

async fn publish(bus: &Bus, to: &'static str) {
    let connected = bus.clone().connect().await.expect("the bus connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced(to, BytesMut::from(&b"x"[..])),
            None,
        )
        .await
        .expect("the bus takes the message");
}

async fn check_cooperation(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    publish(&stand.bus, "bus/spin").await;
    settle().await;
    let spun = SPINS.load(Ordering::Relaxed);
    publish(&stand.bus, "bus/calm").await;
    settle().await;
    let calmed = CALMS.load(Ordering::Relaxed);
    publish(&stand.bus, "bus/kept").await;
    settle().await;
    let kept_spins = KEPT.load(Ordering::Relaxed);
    publish(&stand.bus, "bus/calm").await;
    settle().await;
    let calmed_again = CALMS.load(Ordering::Relaxed);
    publish(&stand.bus, "bus/batch").await;
    settle().await;
    let batch_spins = BATCHES.load(Ordering::Relaxed);
    publish(&stand.bus, "bus/calm").await;
    settle().await;
    let calmed_last = CALMS.load(Ordering::Relaxed);

    app::CONTROL.stop();
    settle().await;

    assert!(spun > 1, "deliveries of the spinning message: {spun}");
    assert_eq!(calmed, 1, "deliveries of the second subscription's message");
    assert!(
        kept_spins > 1,
        "deliveries of the kept-copies message: {kept_spins}"
    );
    assert_eq!(calmed_again, 2, "deliveries once that message spins too");
    assert!(batch_spins > 1, "runs of the batch handler: {batch_spins}");
    assert_eq!(calmed_last, 3, "deliveries once the batch spins too");
    println!("test a spinning subscription does not starve the executor ... ok");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_cooperation(spawner, stand).await;
    stand.done.set(true);
}

fn main() {
    static STAND: StaticCell<Stand> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let stand: &'static Stand = STAND.init(Stand {
        bus: Bus::new(),
        done: Cell::new(false),
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            spawner.spawn(drive(spawner, stand).expect("the driver task is free"));
        },
        || stand.done.get(),
    );
}
