//! Apps with many routes, written with the attributes as a device crate writes them: thirty-two
//! handlers on one broker's scope, and thirty-two spread over two brokers. They build without a
//! `recursion_limit` attribute, and every route gets its own deliveries.
//!
//! An app of several brokers connects them and opens their routes in the order they were
//! registered, shuts them down in the reverse order, and on a broker that refuses shuts down
//! the ones already connected.

// A thread-mode embassy task is never sent anywhere.
#![allow(clippy::future_not_send)]

use core::cell::{Cell, RefCell};
use core::future::ready;
use core::mem::take;
use core::sync::atomic::{AtomicU32, Ordering};
use std::error::Error;
use std::fmt;
use std::sync::Mutex;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use ruststream::memory::local::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemorySource, LocalMemorySubscriber,
};
use ruststream::runtime::{Control, EmbeddedApp, HandlerOutcome, IntoSource, Mounts, RunError};
use ruststream::{
    Broker, BrokerMoves, ConnectedBroker, Deserialized, LocalPublisher, OutgoingMessage,
    SubscriptionSource, subscriber,
};
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The routes of each app.
const ROUTES: usize = 32;

/// The first board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The second board's bus, a broker of its own type.
type Radio = LocalMemoryBroker<2>;

/// A reading as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// The deliveries each route of the one-scope app saw.
static ONE_SCOPE: [AtomicU32; ROUTES] = [const { AtomicU32::new(0) }; ROUTES];

/// The deliveries each route of the two-broker app saw.
static TWO_BROKERS: [AtomicU32; ROUTES] = [const { AtomicU32::new(0) }; ROUTES];

/// One counting handler per route: `name` reads `subject` and counts into `seen[index]`.
macro_rules! counters {
    ($seen:ident: $($name:ident $index:literal $subject:literal),* $(,)?) => {
        $(
            #[subscriber(LocalMemorySource::new($subject))]
            async fn $name(reading: &Reading<'_>) -> HandlerOutcome {
                $seen[$index].fetch_add(u32::from(!reading.0.is_empty()), Ordering::Relaxed);
                HandlerOutcome::ack()
            }
        )*
    };
}

counters!(ONE_SCOPE:
    a0 0 "a/0", a1 1 "a/1", a2 2 "a/2", a3 3 "a/3", a4 4 "a/4", a5 5 "a/5", a6 6 "a/6",
    a7 7 "a/7", a8 8 "a/8", a9 9 "a/9", a10 10 "a/10", a11 11 "a/11", a12 12 "a/12",
    a13 13 "a/13", a14 14 "a/14", a15 15 "a/15", a16 16 "a/16", a17 17 "a/17", a18 18 "a/18",
    a19 19 "a/19", a20 20 "a/20", a21 21 "a/21", a22 22 "a/22", a23 23 "a/23", a24 24 "a/24",
    a25 25 "a/25", a26 26 "a/26", a27 27 "a/27", a28 28 "a/28", a29 29 "a/29", a30 30 "a/30",
    a31 31 "a/31",
);

counters!(TWO_BROKERS:
    b0 0 "b/0", b1 1 "b/1", b2 2 "b/2", b3 3 "b/3", b4 4 "b/4", b5 5 "b/5", b6 6 "b/6",
    b7 7 "b/7", b8 8 "b/8", b9 9 "b/9", b10 10 "b/10", b11 11 "b/11", b12 12 "b/12",
    b13 13 "b/13", b14 14 "b/14", b15 15 "b/15", b16 16 "b/16", b17 17 "b/17", b18 18 "b/18",
    b19 19 "b/19", b20 20 "b/20", b21 21 "b/21", b22 22 "b/22", b23 23 "b/23", b24 24 "b/24",
    b25 25 "b/25", b26 26 "b/26", b27 27 "b/27", b28 28 "b/28", b29 29 "b/29", b30 30 "b/30",
    b31 31 "b/31",
);

#[ruststream::app(runtime = embassy)]
fn one_scope(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(a0)
            .include(a1)
            .include(a2)
            .include(a3)
            .include(a4)
            .include(a5)
            .include(a6)
            .include(a7)
            .include(a8)
            .include(a9)
            .include(a10)
            .include(a11)
            .include(a12)
            .include(a13)
            .include(a14)
            .include(a15)
            .include(a16)
            .include(a17)
            .include(a18)
            .include(a19)
            .include(a20)
            .include(a21)
            .include(a22)
            .include(a23)
            .include(a24)
            .include(a25)
            .include(a26)
            .include(a27)
            .include(a28)
            .include(a29)
            .include(a30)
            .include(a31)
    })
}

#[ruststream::app(runtime = embassy)]
fn two_brokers(bus: Bus, radio: Radio) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new()
        .with_broker(bus, |b| {
            b.include(b0)
                .include(b1)
                .include(b2)
                .include(b3)
                .include(b4)
                .include(b5)
                .include(b6)
                .include(b7)
                .include(b8)
                .include(b9)
                .include(b10)
                .include(b11)
                .include(b12)
                .include(b13)
                .include(b14)
                .include(b15)
        })
        .with_broker(radio, |b| {
            b.include(b16)
                .include(b17)
                .include(b18)
                .include(b19)
                .include(b20)
                .include(b21)
                .include(b22)
                .include(b23)
                .include(b24)
                .include(b25)
                .include(b26)
                .include(b27)
                .include(b28)
                .include(b29)
                .include(b30)
                .include(b31)
        })
}

/// What the recording brokers did, in the order they did it.
static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Records one thing a recording broker did.
fn record(event: String) {
    EVENTS.lock().expect("the log is not poisoned").push(event);
}

/// Takes what the recording brokers did so far.
fn recorded() -> Vec<String> {
    take(&mut *EVENTS.lock().expect("the log is not poisoned"))
}

/// Why a recording broker refused.
#[derive(Debug)]
struct Refused;

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("refused")
    }
}

impl Error for Refused {}

/// A bus that records its connect and its shutdown under `name`, and refuses to connect when
/// told to.
struct Recorder {
    name: &'static str,
    bus: Bus,
    refuses: bool,
}

impl Recorder {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            bus: Bus::new(),
            refuses: false,
        }
    }

    fn refusing(name: &'static str) -> Self {
        Self {
            refuses: true,
            ..Self::new(name)
        }
    }
}

/// A connected [`Recorder`].
struct Recorded {
    name: &'static str,
    connected: ConnectedLocalMemoryBroker<4>,
}

impl Broker for Recorder {
    type Error = Refused;
    type Connected = Recorded;

    async fn connect(self) -> Result<Recorded, Refused> {
        if self.refuses {
            record(format!("refuse {}", self.name));
            return Err(Refused);
        }
        record(format!("connect {}", self.name));
        let connected = self.bus.connect().await.map_err(|_| Refused)?;
        Ok(Recorded {
            name: self.name,
            connected,
        })
    }
}

impl ConnectedBroker for Recorded {
    type Error = Refused;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Refused>> {
        record(format!("close {}", self.name));
        ready(Ok(()))
    }
}

/// A subscription to `subject` that records its opening, and refuses to open when told to.
#[derive(Clone, Copy)]
struct Logged {
    subject: &'static str,
    refuses: bool,
}

impl Logged {
    const fn new(subject: &'static str) -> Self {
        Self {
            subject,
            refuses: false,
        }
    }

    const fn refusing(subject: &'static str) -> Self {
        Self {
            subject,
            refuses: true,
        }
    }
}

impl IntoSource for Logged {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<Recorded> for Logged {
    type Subscriber = LocalMemorySubscriber<4>;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.subject
    }

    async fn subscribe(self, connected: &Recorded) -> Result<Self::Subscriber, Refused> {
        if self.refuses {
            record(format!("refuse {}", self.subject));
            return Err(Refused);
        }
        record(format!("subscribe {}", self.subject));
        LocalMemorySource::new(self.subject)
            .subscribe(&connected.connected)
            .await
            .map_err(|_| Refused)
    }
}

/// A handler of the recording brokers' subscriptions: none of them gets a delivery.
macro_rules! logged {
    ($($name:ident $ctor:ident $subject:literal),* $(,)?) => {
        $(
            #[subscriber(Logged::$ctor($subject))]
            async fn $name(reading: &Reading<'_>) -> HandlerOutcome {
                let _ = reading.0;
                HandlerOutcome::ack()
            }
        )*
    };
}

logged!(
    first0 new "first/0",
    first1 new "first/1",
    first2 new "first/2",
    second0 new "second/0",
    second1 new "second/1",
    second2 new "second/2",
    second3 new "second/3",
    second4 new "second/4",
    third0 new "third/0",
    refused refusing "second/refused",
);

/// Three brokers of three, five and one routes, each scope's routes in the order written.
fn three_brokers() -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new()
        .with_broker(Recorder::new("first"), |b| {
            b.include(first0).include(first1).include(first2)
        })
        .with_broker(Recorder::new("second"), |b| {
            b.include(second0)
                .include(second1)
                .include(second2)
                .include(second3)
                .include(second4)
        })
        .with_broker(Recorder::new("third"), |b| b.include(third0))
}

/// The second broker refuses one subscription, after the first broker connected.
fn refused_subscription() -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new()
        .with_broker(Recorder::new("first"), |b| b.include(first0))
        .with_broker(Recorder::new("second"), |b| {
            b.include(second0).include(refused)
        })
        .with_broker(Recorder::new("third"), |b| b.include(third0))
}

/// The third broker refuses to connect, after the first two connected.
fn refused_connect() -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new()
        .with_broker(Recorder::new("first"), |b| b.include(first0))
        .with_broker(Recorder::new("second"), |b| b.include(second0))
        .with_broker(Recorder::refusing("third"), |b| b.include(third0))
}

/// What the scenarios keep in statics.
struct Stand {
    bus: Bus,
    radio: Radio,
    three: Control,
    refused_subscription: Control,
    refused_connect: Control,
    outcome: RefCell<Option<Result<(), RunError>>>,
    done: Cell<bool>,
}

// A device holds the app's future in its static task and awaits it in place, as this task does:
// nothing moves it.
#[allow(clippy::large_futures)]
#[embassy_executor::task]
async fn run_three(stand: &'static Stand) {
    let outcome = three_brokers().run(&stand.three).await;
    stand.outcome.replace(Some(outcome));
}

// A device holds the app's future in its static task and awaits it in place, as this task does:
// nothing moves it.
#[allow(clippy::large_futures)]
#[embassy_executor::task]
async fn run_refused_subscription(stand: &'static Stand) {
    let outcome = refused_subscription()
        .run(&stand.refused_subscription)
        .await;
    stand.outcome.replace(Some(outcome));
}

// A device holds the app's future in its static task and awaits it in place, as this task does:
// nothing moves it.
#[allow(clippy::large_futures)]
#[embassy_executor::task]
async fn run_refused_connect(stand: &'static Stand) {
    let outcome = refused_connect().run(&stand.refused_connect).await;
    stand.outcome.replace(Some(outcome));
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

/// Publishes one byte to `subject` on `broker`.
async fn publish<const CAP: usize>(broker: &LocalMemoryBroker<CAP>, subject: &str) {
    let connected = broker.clone().connect().await.expect("the broker connects");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced(subject, BytesMut::from(&b"x"[..])),
            None,
        )
        .await
        .expect("the broker takes the message");
}

/// What each route of an app saw.
fn seen(counters: &[AtomicU32; ROUTES]) -> Vec<u32> {
    counters
        .iter()
        .map(|count| count.load(Ordering::Relaxed))
        .collect()
}

async fn check_one_scope(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(one_scope::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;
    for route in 0..ROUTES {
        publish(&stand.bus, &format!("a/{route}")).await;
        settle().await;
    }
    one_scope::CONTROL.stop();
    settle().await;
    assert_eq!(seen(&ONE_SCOPE), vec![1; ROUTES], "deliveries per route");
    passed("thirty-two routes on one scope each run their own deliveries");
}

async fn check_two_brokers(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(
        two_brokers::task(stand.bus.clone(), stand.radio.clone()).expect("the app task is free"),
    );
    settle().await;
    for route in 0..ROUTES / 2 {
        publish(&stand.bus, &format!("b/{route}")).await;
        settle().await;
    }
    for route in ROUTES / 2..ROUTES {
        publish(&stand.radio, &format!("b/{route}")).await;
        settle().await;
    }
    two_brokers::CONTROL.stop();
    settle().await;
    assert_eq!(seen(&TWO_BROKERS), vec![1; ROUTES], "deliveries per route");
    passed("thirty-two routes over two brokers each run their own deliveries");
}

/// What the recording scenarios expect a run to log: every event in `events`, in that order.
fn events(events: &[&str]) -> Vec<String> {
    events.iter().map(ToString::to_string).collect()
}

async fn check_start_order(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(run_three(stand).expect("the app task is free"));
    settle().await;
    let started = recorded();
    stand.three.stop();
    settle().await;
    let stopped = recorded();
    let outcome = stand.outcome.take();

    assert_eq!(
        started,
        events(&[
            "connect first",
            "connect second",
            "connect third",
            "subscribe first/0",
            "subscribe first/1",
            "subscribe first/2",
            "subscribe second/0",
            "subscribe second/1",
            "subscribe second/2",
            "subscribe second/3",
            "subscribe second/4",
            "subscribe third/0",
        ]),
        "what the app did to start"
    );
    passed("brokers connect, then routes open, in the order they were registered");
    assert_eq!(
        stopped,
        events(&["close third", "close second", "close first"]),
        "what the app did to stop"
    );
    assert!(matches!(outcome, Some(Ok(()))), "a stopped app returns Ok");
    passed("brokers shut down in the reverse order");
}

async fn check_refused_subscription(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(run_refused_subscription(stand).expect("the app task is free"));
    settle().await;
    let log = recorded();
    let outcome = stand.outcome.take();

    assert_eq!(
        log,
        events(&[
            "connect first",
            "connect second",
            "connect third",
            "subscribe first/0",
            "subscribe second/0",
            "refuse second/refused",
            "close third",
            "close second",
            "close first",
        ]),
        "what the app did on a refused subscription"
    );
    assert!(
        matches!(outcome, Some(Err(RunError::Subscribe { .. }))),
        "the app reports the refused subscription"
    );
    passed("a refused subscription shuts down every broker already connected");
}

async fn check_refused_connect(spawner: Spawner, stand: &'static Stand) {
    spawner.spawn(run_refused_connect(stand).expect("the app task is free"));
    settle().await;
    let log = recorded();
    let outcome = stand.outcome.take();

    assert_eq!(
        log,
        events(&[
            "connect first",
            "connect second",
            "refuse third",
            "close second",
            "close first",
        ]),
        "what the app did on a refused connect"
    );
    assert!(
        matches!(outcome, Some(Err(RunError::Connect { .. }))),
        "the app reports the refused connect"
    );
    passed("a refused connect shuts down every broker already connected");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_one_scope(spawner, stand).await;
    check_two_brokers(spawner, stand).await;
    check_start_order(spawner, stand).await;
    check_refused_subscription(spawner, stand).await;
    check_refused_connect(spawner, stand).await;
    stand.done.set(true);
}

fn main() {
    static STAND: StaticCell<Stand> = StaticCell::new();
    static EXECUTOR: StaticCell<Executor> = StaticCell::new();

    let stand: &'static Stand = STAND.init(Stand {
        bus: Bus::new(),
        radio: Radio::new(),
        three: Control::new(),
        refused_subscription: Control::new(),
        refused_connect: Control::new(),
        outcome: RefCell::new(None),
        done: Cell::new(false),
    });
    EXECUTOR.init(Executor::new()).run_until(
        |spawner: Spawner| {
            spawner.spawn(drive(spawner, stand).expect("the driver task is free"));
        },
        || stand.done.get(),
    );
}
