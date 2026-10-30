//! A device service written with the attributes, as a device crate writes it: handlers with
//! `#[subscriber]`, the app with `#[ruststream::app]`, and the app's task spawned on embassy's
//! executor.
//!
//! The bodies hold values that are not `Send` across an `.await`, which compiles only because
//! the attributes emit the local forms on this runtime. The driver task publishes on the local
//! bus, reads what the app did, and stops it through the control the attribute emits.

// A thread-mode embassy task is never sent anywhere, and the bodies hold values that are not
// `Send` on purpose.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::num::NonZeroUsize;
use core::pin::pin;
use core::sync::atomic::{AtomicU32, Ordering};
use std::rc::Rc;

use bytes::BytesMut;
use embassy_executor::{Executor, SpawnError, Spawner};
use embassy_futures::yield_now;
use futures::{FutureExt, StreamExt};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Health, Mounts, Out, SubscriberSettings};
use ruststream::{
    Deserialized, LocalBroker, LocalIncomingMessage, LocalPublisher, LocalSubscriber,
    LocalSubscriptionSource, OutSlot, Outgoing, OutgoingMessage, Publisher, Serialized, subscriber,
};
use serde::Serialize;
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A reading as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// An alert the level handler raises: the level's bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bus/alerts")]
struct Alert(Vec<u8>);

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// The levels the level handler saw.
static LEVELS: AtomicU32 = AtomicU32::new(0);

/// Raises an alert for every level through its slot, holding a value that is not `Send` across
/// the publish.
#[subscriber(LocalMemorySource::new("bus/levels"))]
async fn on_level(level: &Reading<'_>, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    let held = Rc::new(level.0.to_vec());
    yield_now().await;
    LEVELS.fetch_add(1, Ordering::Relaxed);
    match alerts.message(&Alert(held.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// The readings the plain handler saw.
static PLAIN: AtomicU32 = AtomicU32::new(0);

/// Counts every reading, holding a value that is not `Send` across an await: the eager form.
#[subscriber(LocalMemorySource::new("bus/plain"))]
async fn on_plain(reading: &Reading<'_>) -> HandlerOutcome {
    let held = Rc::new(reading.0.len());
    yield_now().await;
    PLAIN.fetch_add(u32::from(*held > 0), Ordering::Relaxed);
    HandlerOutcome::ack()
}

/// The reply to a ping: the ping's bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bus/pong")]
struct Pong(Vec<u8>);

/// Answers a ping with its own bytes, byte for byte: the raw reply form.
#[subscriber(LocalMemorySource::new("bus/ping"), reply)]
async fn on_ping(ping: &Reading<'_>) -> Pong {
    let held = Rc::new(ping.0.to_vec());
    yield_now().await;
    Pong(held.to_vec())
}

/// The reply to a measure: how many bytes it had, encoded by the codec.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "bus/measured")]
struct Measured {
    bytes: usize,
}

/// Measures a reading and answers through the codec: the reply form.
#[subscriber(LocalMemorySource::new("bus/measure"), reply)]
async fn on_measure(reading: &Reading<'_>) -> Measured {
    let held = Rc::new(reading.0.len());
    yield_now().await;
    Measured { bytes: *held }
}

/// The readings the batch handler saw, over every batch.
static BATCHED: AtomicU32 = AtomicU32::new(0);

/// Counts the readings of every batch: the batch form.
#[subscriber(LocalMemorySource::new("bus/batches"))]
async fn on_batch(readings: &[Reading<'_>]) -> HandlerOutcome {
    let held = Rc::new(readings.len());
    yield_now().await;
    BATCHED.fetch_add(u32::try_from(*held).unwrap_or(u32::MAX), Ordering::Relaxed);
    HandlerOutcome::ack()
}

/// The size the batch handler's subscription reads its batches at.
const BATCH: NonZeroUsize = NonZeroUsize::new(2).expect("two is not zero");

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_level)
            .out(Alerts, LocalMemoryPublish)
            .include(on_plain)
            .include(on_ping)
            .include(on_measure)
            .include(on_batch.batch(BATCH))
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

/// Reports one passed check the way the test harness does.
fn passed(check: &str) {
    println!("test {check} ... ok");
}

async fn publish(bus: &Bus, subject: &'static str, payload: &[u8]) {
    let connected = bus.clone().connect().await.expect("the bus connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced(subject, BytesMut::from(payload)),
            None,
        )
        .await
        .expect("the bus takes the message");
}

/// The payload of the next message `subscriber` holds, if it holds one now.
fn next_payload<Sub: LocalSubscriber>(subscriber: &mut Sub) -> Option<Vec<u8>> {
    pin!(subscriber.stream())
        .next()
        .now_or_never()
        .flatten()
        .and_then(Result::ok)
        .map(|message| message.payload().to_vec())
}

async fn check_the_app(spawner: Spawner, stand: &'static Stand) {
    let connected = stand.bus.clone().connect().await.expect("the bus connects");
    let mut alerts = LocalMemorySource::new("bus/alerts")
        .subscribe(&connected)
        .await
        .expect("the alerts subscribe");
    let mut pongs = LocalMemorySource::new("bus/pong")
        .subscribe(&connected)
        .await
        .expect("the pongs subscribe");
    let mut measured = LocalMemorySource::new("bus/measured")
        .subscribe(&connected)
        .await
        .expect("the measures subscribe");
    let starting = app::CONTROL.health();
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;
    let running = app::CONTROL.health();
    let busy = matches!(app::task(stand.bus.clone()), Err(SpawnError::Busy));

    publish(&stand.bus, "bus/levels", b"high").await;
    publish(&stand.bus, "bus/plain", b"one").await;
    publish(&stand.bus, "bus/ping", b"ping").await;
    publish(&stand.bus, "bus/measure", b"four").await;
    publish(&stand.bus, "bus/batches", b"a").await;
    publish(&stand.bus, "bus/batches", b"b").await;
    settle().await;
    let alert = next_payload(&mut alerts);
    let pong = next_payload(&mut pongs);
    let measure = next_payload(&mut measured);

    app::CONTROL.stop();
    settle().await;
    let stopped = app::CONTROL.health();

    assert_eq!(starting, Health::Starting, "the health before the task ran");
    assert_eq!(
        running,
        Health::Running,
        "the health once the task opened every route"
    );
    assert!(busy, "the app's task runs once spawned");
    passed("the app's task runs the app and reports Running");
    assert_eq!(
        LEVELS.load(Ordering::Relaxed),
        1,
        "levels the attribute's handler saw"
    );
    passed("a handler the attribute made runs on the embedded app");
    assert_eq!(
        alert,
        Some(b"high".to_vec()),
        "the alert the level handler published"
    );
    passed("a handler publishes through its Out slot");
    assert_eq!(
        PLAIN.load(Ordering::Relaxed),
        1,
        "readings the eager form saw"
    );
    passed("a handler without a slot runs on the embedded app");
    assert_eq!(pong, Some(b"ping".to_vec()), "the raw reply");
    passed("a handler replies byte for byte");
    assert_eq!(
        measure,
        Some(br#"{"bytes":4}"#.to_vec()),
        "the reply the codec encoded"
    );
    passed("a handler replies through the codec");
    assert_eq!(
        BATCHED.load(Ordering::Relaxed),
        2,
        "readings the batch form saw"
    );
    passed("a batch handler runs on the embedded app");
    assert_eq!(stopped, Health::Stopped, "the health after the stop");
    // The pool holds one task: a fresh one is free only once the first returned. Taken after
    // every other check, so a failing one does not drop an unspawned token.
    let token = app::task(stand.bus.clone()).expect("the app's task ended");
    spawner.spawn(token);
    passed("the control's stop ends the app's task");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_the_app(spawner, stand).await;
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
