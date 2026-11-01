//! The embedded runner's plan steps on the publishing forms: `.pool::<N>()`, `.keyed::<N>()` and
//! `.retry_capacity::<CAP>()` after a handler that replies through the codec, one that replies
//! byte for byte, and one that publishes through an `Out` slot.
//!
//! The handlers are written with the attributes and wait on embassy-time's mock clock. The
//! driver task publishes on the local bus, reads how many deliveries were in flight at once and
//! what the handlers published, and moves the clock.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::pin::pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_time::{Duration as Ticks, MockDriver};
use futures::{FutureExt, StreamExt};
use ruststream::IncomingMessage;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{
    Clock, EmbassyClock, EmbeddedApp, HandlerOutcome, MountSteps, Mounts, Out,
};
use ruststream::{
    Broker, Deserialized, LocalPublisher, LocalSettle, LocalSubscriber, OutSlot, Outgoing,
    OutgoingMessage, Publisher, Serialized, SubscriptionSource, subscriber,
};
use serde::Serialize;
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// How long every handler works on the clock.
const WORK: Duration = Duration::from_secs(10);

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A reading as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// A counter of the deliveries a handler holds at once, and the most it ever held.
struct InFlight {
    now: AtomicU32,
    peak: AtomicU32,
}

impl InFlight {
    const fn new() -> Self {
        Self {
            now: AtomicU32::new(0),
            peak: AtomicU32::new(0),
        }
    }

    /// Holds one delivery for [`WORK`] on the clock.
    async fn work(&self) {
        let now = self.now.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        EmbassyClock.sleep(WORK).await;
        self.now.fetch_sub(1, Ordering::Relaxed);
    }

    fn peak(&self) -> u32 {
        self.peak.load(Ordering::Relaxed)
    }
}

/// The reply to a measure: how many bytes it had, encoded by the codec.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "bus/measured")]
struct Measured {
    bytes: usize,
}

static MEASURING: InFlight = InFlight::new();

/// Measures a reading after a while and answers through the codec.
#[subscriber(LocalMemorySource::new("bus/measure"), reply)]
async fn on_measure(reading: &Reading<'_>) -> Measured {
    let bytes = reading.0.len();
    MEASURING.work().await;
    Measured { bytes }
}

/// The reply to a ping: its bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bus/pong")]
struct Pong(Vec<u8>);

static PINGING: InFlight = InFlight::new();

/// Answers a ping after a while, byte for byte.
#[subscriber(LocalMemorySource::new("bus/ping"), reply)]
async fn on_ping(ping: &Reading<'_>) -> Pong {
    let bytes = ping.0.to_vec();
    PINGING.work().await;
    Pong(bytes)
}

/// An alert: the level's bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bus/alerts")]
struct Alert(Vec<u8>);

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// The levels the alert handler saw.
static LEVELS: AtomicU32 = AtomicU32::new(0);

/// Asks for a retry of a level's first delivery, and raises an alert on the next.
#[subscriber(LocalMemorySource::new("bus/levels"))]
async fn on_level(level: &Reading<'_>, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    if LEVELS.fetch_add(1, Ordering::Relaxed) == 0 {
        return HandlerOutcome::retry_after(WORK);
    }
    match alerts.message(&Alert(level.0.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_measure)
            .pool::<2>()
            .include(on_ping)
            .keyed::<2>()
            .include(on_level)
            .out(Alerts, LocalMemoryPublish)
            .retry_capacity::<1>()
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

/// Moves the mock clock and lets what fell due run.
async fn advance(by: Duration) {
    MockDriver::get().advance(Ticks::try_from(by).expect("the step fits the clock"));
    settle().await;
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

/// The payloads `subscriber` holds now, each acked.
async fn payloads<Sub: LocalSubscriber>(subscriber: &mut Sub) -> Vec<Vec<u8>> {
    let mut stream = pin!(subscriber.stream());
    let mut held = Vec::new();
    while let Some(Some(Ok(message))) = stream.next().now_or_never() {
        held.push(message.payload().to_vec());
        message.ack().await.expect("the bus takes the ack");
    }
    held
}

async fn check_the_plans(spawner: Spawner, stand: &'static Stand) {
    let connected = stand.bus.clone().connect().await.expect("the bus connects");
    let mut measure_replies = LocalMemorySource::new("bus/measured")
        .subscribe(&connected)
        .await
        .expect("the measures subscribe");
    let mut pongs = LocalMemorySource::new("bus/pong")
        .subscribe(&connected)
        .await
        .expect("the pongs subscribe");
    let mut alerts = LocalMemorySource::new("bus/alerts")
        .subscribe(&connected)
        .await
        .expect("the alerts subscribe");
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    publish(&stand.bus, "bus/measure", b"one").await;
    publish(&stand.bus, "bus/measure", b"four").await;
    publish(&stand.bus, "bus/ping", b"a").await;
    publish(&stand.bus, "bus/ping", b"b").await;
    publish(&stand.bus, "bus/levels", b"high").await;
    settle().await;
    let levels_before = LEVELS.load(Ordering::Relaxed);
    advance(WORK).await;
    let mut measures = payloads(&mut measure_replies).await;
    measures.sort();
    let mut pinged = payloads(&mut pongs).await;
    pinged.sort();
    let alerted = payloads(&mut alerts).await;

    app::CONTROL.stop();
    settle().await;

    assert_eq!(MEASURING.peak(), 2, "measures in flight at once");
    assert_eq!(
        measures,
        [br#"{"bytes":3}"#.to_vec(), br#"{"bytes":4}"#.to_vec()],
        "the replies of both measures, one work period in"
    );
    passed("a pool of two runs a codec reply handler's deliveries at once");
    assert_eq!(PINGING.peak(), 2, "pings in flight at once");
    assert_eq!(
        pinged,
        [b"a".to_vec(), b"b".to_vec()],
        "the replies of both pings, one work period in"
    );
    passed("two keyed lanes run a raw reply handler's deliveries at once");
    assert_eq!(
        levels_before, 1,
        "deliveries of the level before its copy is due"
    );
    assert_eq!(
        alerted,
        [b"high".to_vec()],
        "the alert the kept copy raised once due"
    );
    passed("a kept copy of an Out slot handler's delivery fires on the clock");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_the_plans(spawner, stand).await;
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
