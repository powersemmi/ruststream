//! An `and_after` continuation on the embedded runner: it runs once the delivery is settled, and
//! the runner keeps it in the one box the outcome made, not a second one around it.
//!
//! The driver task publishes on the local bus and counts the allocations of a delivery that
//! leaves a continuation against one that leaves none.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};
use std::alloc::System;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::{Broker, Deserialized, OutgoingMessage, Publisher, subscriber};
use static_cell::StaticCell;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// What the continuations added up.
static RAN: AtomicU32 = AtomicU32::new(0);

/// Acks every job and leaves nothing behind.
#[subscriber(LocalMemorySource::new("bus/plain"))]
async fn on_plain(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

/// Acks every job and leaves a continuation behind, one word wide.
#[subscriber(LocalMemorySource::new("bus/after"))]
async fn on_after(job: &Job<'_>) -> HandlerOutcome {
    let weight = u32::from(job.0[0]);
    HandlerOutcome::ack().and_after(async move {
        RAN.fetch_add(weight, Ordering::Relaxed);
    })
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_plain).include(on_after))
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

/// Reports one passed check the way the test harness does.
fn passed(check: &str) {
    println!("test {check} ... ok");
}

/// Allocations one delivery makes on its way through the app, its publish excluded.
async fn allocations_of(bus: &Bus, topic: &'static str, job: u8) -> usize {
    let connected = bus.clone().connect().await.expect("the bus connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced(topic, BytesMut::from(&[job][..])),
            None,
        )
        .await
        .expect("the bus takes the message");
    let delivery = Region::new(ALLOCATOR);
    settle().await;
    delivery.change().allocations
}

async fn check_continuations(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    // The first continuation sizes the host's lists; the ones measured come after it.
    allocations_of(&stand.bus, "bus/after", 1).await;
    allocations_of(&stand.bus, "bus/plain", 1).await;
    let ran_before = RAN.load(Ordering::Relaxed);

    let plain = allocations_of(&stand.bus, "bus/plain", 1).await;
    let after = allocations_of(&stand.bus, "bus/after", 5).await;
    let ran = RAN.load(Ordering::Relaxed) - ran_before;

    app::CONTROL.stop();
    settle().await;

    assert_eq!(ran, 5, "what the measured continuation added up");
    passed("a continuation runs once its delivery is settled");
    assert_eq!(
        after - plain,
        1,
        "allocations a continuation adds to a delivery: the outcome's box alone"
    );
    passed("a continuation is boxed once");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_continuations(spawner, stand).await;
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
