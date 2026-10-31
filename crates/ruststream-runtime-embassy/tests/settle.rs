//! How the embedded runner settles what a handler returns, beyond a plain ack: the work an
//! outcome leaves behind, and a `retry_after` on a registration with no room for its copy or
//! with no delay.
//!
//! The handlers are written with the attributes. The driver task publishes on the local bus,
//! reads what the handlers saw and what they replied, and stops the app.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::pin::pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use futures::{FutureExt, StreamExt};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::{
    Deserialized, LocalBroker, LocalIncomingMessage, LocalPublisher, LocalSubscriber,
    LocalSubscriptionSource, Outgoing, OutgoingMessage, Serialized, subscriber,
};
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The delay the no-room handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// The continuations the plain registration's outcomes left, once run.
static AFTER: AtomicU32 = AtomicU32::new(0);

/// Acks every job and leaves a continuation behind.
#[subscriber(LocalMemorySource::new("bus/after"))]
async fn on_after(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack().and_after(async {
        AFTER.fetch_add(1, Ordering::Relaxed);
    })
}

/// The continuations the registration with a store left, once run.
static KEPT_AFTER: AtomicU32 = AtomicU32::new(0);

/// Acks every job and leaves a continuation behind, on a registration with room for copies.
#[subscriber(LocalMemorySource::new("bus/kept-after"))]
async fn on_kept_after(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack().and_after(async {
        KEPT_AFTER.fetch_add(1, Ordering::Relaxed);
    })
}

/// The deliveries the no-room handler saw.
static LATER_SEEN: AtomicU32 = AtomicU32::new(0);

/// Asks for a later retry of the first delivery, on a registration with no room for its copy.
#[subscriber(LocalMemorySource::new("bus/later"))]
async fn on_later(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    if LATER_SEEN.fetch_add(1, Ordering::Relaxed) == 0 {
        HandlerOutcome::retry_after(LATER)
    } else {
        HandlerOutcome::ack()
    }
}

/// The deliveries the handler with a store saw.
static NOW_SEEN: AtomicU32 = AtomicU32::new(0);

/// The deliveries of the handler with a store that came as a copy, with a retry count.
static NOW_COPIES: AtomicU32 = AtomicU32::new(0);

/// Asks for a retry of the first delivery with no delay, on a registration with room for copies.
#[subscriber(LocalMemorySource::new("bus/now"))]
async fn on_now(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    let copied = ctx.headers().get(RETRY_COUNT_HEADER).is_some();
    NOW_COPIES.fetch_add(u32::from(copied), Ordering::Relaxed);
    if NOW_SEEN.fetch_add(1, Ordering::Relaxed) == 0 {
        HandlerOutcome::retry_after(Duration::ZERO)
    } else {
        HandlerOutcome::ack()
    }
}

/// The reply to a ping: its bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bus/pong")]
struct Pong(Vec<u8>);

/// Answers a ping byte for byte: the scope ends on its open mount chain.
#[subscriber(LocalMemorySource::new("bus/ping"), reply)]
async fn on_ping(ping: &Job<'_>) -> Pong {
    Pong(ping.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_after)
            .include(on_kept_after)
            .pool::<2>()
            .retry_capacity::<1>()
            .include(on_later)
            .include(on_now)
            .retry_capacity::<1>()
            .include(on_ping)
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

async fn check_settling(spawner: Spawner, stand: &'static Stand) {
    let connected = stand.bus.clone().connect().await.expect("the bus connects");
    let mut pongs = LocalMemorySource::new("bus/pong")
        .subscribe(&connected)
        .await
        .expect("the pongs subscribe");
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    publish(&stand.bus, "bus/after", b"a").await;
    publish(&stand.bus, "bus/kept-after", b"k").await;
    publish(&stand.bus, "bus/later", b"l").await;
    publish(&stand.bus, "bus/now", b"n").await;
    publish(&stand.bus, "bus/ping", b"p").await;
    settle().await;
    let pong = next_payload(&mut pongs);

    app::CONTROL.stop();
    settle().await;

    assert_eq!(
        AFTER.load(Ordering::Relaxed),
        1,
        "continuations run on the plain registration"
    );
    passed("an outcome's continuation runs once the delivery is settled");
    assert_eq!(
        KEPT_AFTER.load(Ordering::Relaxed),
        1,
        "continuations run on the registration with a store"
    );
    passed("an outcome's continuation runs on a registration that keeps its copies");
    assert_eq!(
        LATER_SEEN.load(Ordering::Relaxed),
        2,
        "deliveries of the job whose retry found no room, the clock not moved"
    );
    passed("a retry_after with no room for its copy requeues the delivery at once");
    assert_eq!(
        NOW_SEEN.load(Ordering::Relaxed),
        2,
        "deliveries of the job whose retry asked for no delay, the clock not moved"
    );
    assert_eq!(
        NOW_COPIES.load(Ordering::Relaxed),
        1,
        "deliveries of that job that came as a copy carrying its retry count"
    );
    passed("a retry_after with no delay publishes the kept copy at once");
    assert_eq!(
        pong,
        Some(b"p".to_vec()),
        "the reply of the last registration"
    );
    passed("a scope may end on a registration's open mount chain");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_settling(spawner, stand).await;
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
