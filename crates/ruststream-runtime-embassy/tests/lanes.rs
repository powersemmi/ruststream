//! The embedded runner's constant worker pool and keyed lanes, inside a whole app on embassy's
//! executor, the handlers waiting on embassy-time's mock clock.
//!
//! The broker is a stand-in whose frames carry a partition key. Each scenario spawns an app,
//! publishes, moves the clock and reads which deliveries were in flight at once and in which
//! order each key's deliveries ran.

// A thread-mode embassy task is never sent anywhere, and the apps hold brokers that are not
// `Send` on purpose.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::convert::Infallible;
use core::future::ready;
use core::pin::Pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context as TaskContext, Poll};
use core::time::Duration;
use std::alloc::System;
use std::sync::Mutex;

use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::{Channel, Receiver};
use embassy_time::{Duration as Ticks, MockDriver};
use futures_core::Stream;
use ruststream_core::{
    AckError, BrokerMoves, HeaderMap, LocalBroker, LocalConnectedBroker, LocalIncomingMessage,
    LocalSubscriber, LocalSubscriptionSource,
};
use ruststream_runtime::{
    Clock, Context, Deserialized, HandlerOutcome, Input, IntoSource, LocalHandle, SoloDeserialized,
    local_subscriber,
};
use ruststream_runtime_embassy::{Control, EmbassyClock, EmbeddedApp, MountSteps, Mounts};
use static_cell::StaticCell;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const SETTLE_ROUNDS: usize = 8;

/// How long every handler works on the clock.
const WORK: Duration = Duration::from_secs(10);

/// A frame: its partition key and one byte of payload, its sequence number.
#[derive(Debug, Clone, Copy)]
struct Keyed {
    key: Option<&'static [u8]>,
    seq: [u8; 1],
}

type Wire = Channel<NoopRawMutex, Keyed, 8>;

/// A delivery of the keyed stand-in, acked into a counter.
#[derive(Debug)]
struct KeyedDelivery {
    frame: Keyed,
    headers: HeaderMap,
    acked: &'static Cell<u32>,
}

impl LocalIncomingMessage for KeyedDelivery {
    fn payload(&self) -> &[u8] {
        &self.frame.seq
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.frame.key
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        self.acked.set(self.acked.get() + 1);
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }
}

/// The stand-in broker over a `static` channel.
struct KeyedBroker {
    wire: &'static Wire,
    acked: &'static Cell<u32>,
}

struct KeyedConnected {
    wire: &'static Wire,
    acked: &'static Cell<u32>,
}

struct KeyedSubscriber {
    receiver: Receiver<'static, NoopRawMutex, Keyed, 8>,
    acked: &'static Cell<u32>,
}

struct Deliveries<'a>(&'a KeyedSubscriber);

impl Stream for Deliveries<'_> {
    type Item = Result<KeyedDelivery, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let acked = self.0.acked;
        self.0.receiver.poll_receive(cx).map(|frame| {
            Some(Ok(KeyedDelivery {
                frame,
                headers: HeaderMap::new(),
                acked,
            }))
        })
    }
}

impl LocalSubscriber for KeyedSubscriber {
    type Message = KeyedDelivery;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<KeyedDelivery, Infallible>> + '_ {
        Deliveries(self)
    }
}

impl LocalBroker for KeyedBroker {
    type Error = Infallible;
    type Connected = KeyedConnected;

    fn connect(self) -> impl Future<Output = Result<KeyedConnected, Infallible>> {
        ready(Ok(KeyedConnected {
            wire: self.wire,
            acked: self.acked,
        }))
    }
}

impl LocalConnectedBroker for KeyedConnected {
    type Error = Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
struct KeyedSource;

impl LocalSubscriptionSource<KeyedConnected> for KeyedSource {
    type Subscriber = KeyedSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &'static str {
        "keyed"
    }

    fn subscribe(
        self,
        connected: &KeyedConnected,
    ) -> impl Future<Output = Result<KeyedSubscriber, Infallible>> {
        ready(Ok(KeyedSubscriber {
            receiver: connected.wire.receiver(),
            acked: connected.acked,
        }))
    }
}

impl IntoSource for KeyedSource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// The sequence number a frame carries.
struct Seq(u8);

impl Deserialized for Seq {
    type Output<'a> = Self;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Self, Infallible> {
        Ok(Self(payload[0]))
    }
}

impl Input for Seq {
    type Axis = SoloDeserialized<Self>;
}

/// What the handlers record: how many run at once, the most there ever were, and the order the
/// deliveries started in.
struct Record {
    in_flight: AtomicU32,
    peak: AtomicU32,
    started: Mutex<Vec<u8>>,
}

impl Record {
    const fn new() -> Self {
        Self {
            in_flight: AtomicU32::new(0),
            peak: AtomicU32::new(0),
            started: Mutex::new(Vec::new()),
        }
    }

    fn reset(&self) {
        self.peak.store(0, Ordering::Relaxed);
        self.started.lock().expect("unpoisoned").clear();
    }
}

/// Works on the clock for [`WORK`], recording itself.
struct Slow {
    record: &'static Record,
}

impl LocalHandle<Seq> for Slow {
    fn handle(
        &self,
        seq: &Seq,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let record = self.record;
        let now = record.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        record.peak.fetch_max(now, Ordering::Relaxed);
        record.started.lock().expect("unpoisoned").push(seq.0);
        async move {
            EmbassyClock.sleep(WORK).await;
            record.in_flight.fetch_sub(1, Ordering::Relaxed);
            Ok(())
        }
    }
}

struct Stand {
    pooled_wire: Wire,
    pooled_acked: Cell<u32>,
    pooled: Control,
    keyed_wire: Wire,
    keyed_acked: Cell<u32>,
    keyed: Control,
    record: Record,
    done: Cell<bool>,
}

fn pooled(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
    let broker = KeyedBroker {
        wire: &stand.pooled_wire,
        acked: &stand.pooled_acked,
    };
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            local_subscriber(
                KeyedSource,
                Slow {
                    record: &stand.record,
                },
            )
            .build(),
        )
        .pool::<4>()
    })
}

fn keyed(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
    let broker = KeyedBroker {
        wire: &stand.keyed_wire,
        acked: &stand.keyed_acked,
    };
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            local_subscriber(
                KeyedSource,
                Slow {
                    record: &stand.record,
                },
            )
            .build(),
        )
        .keyed::<2>()
    })
}

// Four workers each hold a delivery's whole dispatch, so the app's future is as large as four
// handlers in flight: an embassy task holds it in place, and nothing moves it.
#[allow(clippy::large_futures)]
#[embassy_executor::task]
async fn run_pooled(stand: &'static Stand) {
    pooled(stand)
        .run(&stand.pooled)
        .await
        .expect("the app stops cleanly");
}

#[embassy_executor::task]
async fn run_keyed(stand: &'static Stand) {
    keyed(stand)
        .run(&stand.keyed)
        .await
        .expect("the app stops cleanly");
}

async fn settle() {
    for _ in 0..SETTLE_ROUNDS {
        yield_now().await;
    }
}

async fn advance(by: Duration) {
    MockDriver::get().advance(Ticks::try_from(by).expect("the step fits the clock"));
    settle().await;
}

fn passed(check: &str) {
    println!("test {check} ... ok");
}

async fn check_pool(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(run_pooled(stand).expect("the app task is free"));
    settle().await;
    // The record's own buffer grows before the count starts: it is the test's, not the pool's.
    stand.record.started.lock().expect("unpoisoned").reserve(16);
    let delivery_path = Region::new(ALLOCATOR);
    for seq in 0..5 {
        stand
            .pooled_wire
            .send(Keyed {
                key: None,
                seq: [seq],
            })
            .await;
    }
    settle().await;
    let busy = stand.record.in_flight.load(Ordering::Relaxed);
    let held_by_subscription = stand.pooled_wire.len();
    advance(WORK).await;
    let after_first = (
        stand.pooled_acked.get(),
        stand.record.in_flight.load(Ordering::Relaxed),
    );
    advance(WORK).await;
    let allocations = delivery_path.change().allocations;
    let after_second = stand.pooled_acked.get();
    let peak = stand.record.peak.load(Ordering::Relaxed);
    stand.pooled.stop();
    settle().await;

    assert_eq!(busy, 4, "deliveries in flight while the handlers wait");
    passed("a pool of four runs four deliveries at once");
    assert_eq!(
        after_first,
        (4, 1),
        "acked, and in flight, once the first four finished"
    );
    assert_eq!(after_second, 5, "acked once the fifth finished");
    assert_eq!(peak, 4, "the most deliveries ever in flight");
    assert_eq!(
        held_by_subscription, 1,
        "deliveries the subscription still holds while every worker is busy"
    );
    passed("a fifth delivery waits for a free worker");
    assert_eq!(
        allocations, 0,
        "allocations of five deliveries through the pool"
    );
    passed("the pool's delivery path allocates nothing");
}

async fn check_keyed(spawner: Spawner, stand: &'static Stand) {
    stand.record.reset();
    spawner.spawn(run_keyed(stand).expect("the app task is free"));
    settle().await;
    // Two keys that hash to different lanes of two, each sent twice in a row.
    let (first, second): (&'static [u8], &'static [u8]) = (b"a", b"b");
    for (key, seq) in [(first, 1), (first, 2), (second, 3), (second, 4)] {
        stand
            .keyed_wire
            .send(Keyed {
                key: Some(key),
                seq: [seq],
            })
            .await;
    }
    settle().await;
    let started_early = stand.record.started.lock().expect("unpoisoned").clone();
    advance(WORK).await;
    advance(WORK).await;
    let started = stand.record.started.lock().expect("unpoisoned").clone();
    let acked = stand.keyed_acked.get();
    stand.keyed.stop();
    settle().await;

    assert_eq!(
        started_early,
        [1, 3],
        "the first delivery of each key runs at once"
    );
    passed("deliveries of different keys run on their own lanes at once");
    assert_eq!(started, [1, 3, 2, 4], "the order the deliveries started in");
    assert_eq!(acked, 4, "deliveries acked");
    passed("the deliveries of one key run in order, one at a time");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_pool(spawner, stand).await;
    check_keyed(spawner, stand).await;
    stand.done.set(true);
}

fn main() {
    static STAND: StaticCell<Stand> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let stand: &'static Stand = STAND.init(Stand {
        pooled_wire: Wire::new(),
        pooled_acked: Cell::new(0),
        pooled: Control::new(),
        keyed_wire: Wire::new(),
        keyed_acked: Cell::new(0),
        keyed: Control::new(),
        record: Record::new(),
        done: Cell::new(false),
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            spawner.spawn(drive(spawner, stand).expect("the driver task is free"));
        },
        || stand.done.get(),
    );
}
