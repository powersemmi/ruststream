//! The input forms a device handler takes, each on the embedded runner: a value the codec
//! decodes, a `Message<Headers, Payload>` pair and a `#[derive(InMessages)]` set, without a reply
//! and with one.
//!
//! The handlers are written with the attributes, as a device crate writes them. The driver task
//! publishes JSON on the local bus, reads what each handler saw and what it replied, and stops
//! the app.

// A thread-mode embassy task is never sent anywhere, and the app holds a bus that is not `Send`.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::pin::pin;
use core::sync::atomic::{AtomicU32, Ordering};

use bytes::BytesMut;
use embassy_executor::{Executor, Spawner};
use embassy_futures::yield_now;
use futures::{FutureExt, StreamExt};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Message, Mounts};
use ruststream::{
    Broker, HeaderMap, InMessages, IncomingMessage, LocalPublisher, LocalSubscriber, MessageInfo,
    Outgoing, OutgoingMessage, SubscriptionSource, subscriber,
};
use serde::{Deserialize, Serialize};
use static_cell::StaticCell;

/// Executor rounds a step yields: a round polls every task woken before it began.
const SETTLE_ROUNDS: usize = 8;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// An order, decoded by the codec.
#[derive(Deserialize, MessageInfo)]
struct Order {
    id: u32,
}

/// The header contract an order travels with.
#[derive(Deserialize)]
struct Meta {
    tenant: String,
}

/// A command for the valve: opened or closed, each decoded by the codec.
#[derive(Deserialize, MessageInfo)]
struct Opened {
    opened: u32,
}

/// See [`Opened`].
#[derive(Deserialize, MessageInfo)]
struct Closed {
    closed: u32,
}

/// The set a valve handler takes: the first model that decodes the payload.
#[derive(InMessages)]
enum Valve {
    Opened(Opened),
    Closed(Closed),
}

impl Valve {
    const fn id(&self) -> u32 {
        match self {
            Self::Opened(opened) => opened.opened,
            Self::Closed(closed) => closed.closed,
        }
    }
}

/// What a handler saw: the sum of the ids, one counter per form.
static SEEN: [AtomicU32; 3] = [const { AtomicU32::new(0) }; 3];

fn saw(form: usize, id: u32) -> HandlerOutcome {
    SEEN[form].fetch_add(id, Ordering::Relaxed);
    HandlerOutcome::ack()
}

fn seen(form: usize) -> u32 {
    SEEN[form].load(Ordering::Relaxed)
}

#[subscriber(LocalMemorySource::new("in/order"))]
async fn on_order(order: &Order) -> HandlerOutcome {
    saw(0, order.id)
}

#[subscriber(LocalMemorySource::new("in/pair"))]
async fn on_pair(pair: &Message<Meta, Order>) -> HandlerOutcome {
    saw(1, pair.body.id * u32::from(pair.headers.tenant == "acme"))
}

#[subscriber(LocalMemorySource::new("in/valve"))]
async fn on_valve(valve: &Valve) -> HandlerOutcome {
    saw(2, valve.id())
}

/// The reply every replying handler sends: the id it read.
#[derive(Outgoing, Serialize)]
struct Quote {
    id: u32,
}

#[subscriber(LocalMemorySource::new("in/quote-order"), reply("out/order"))]
async fn quote_order(order: &Order) -> Quote {
    Quote { id: order.id }
}

#[subscriber(LocalMemorySource::new("in/quote-pair"), reply("out/pair"))]
async fn quote_pair(pair: &Message<Meta, Order>) -> Quote {
    Quote { id: pair.body.id }
}

#[subscriber(LocalMemorySource::new("in/quote-valve"), reply("out/valve"))]
async fn quote_valve(valve: &Valve) -> Quote {
    Quote { id: valve.id() }
}

/// The header contract a receipt leaves with.
#[derive(Serialize)]
struct ReceiptMeta {
    order: u32,
}

/// Answers an order with a reply that carries its own header contract.
#[subscriber(LocalMemorySource::new("in/receipt"), reply("out/receipt"))]
async fn receipt(order: &Order) -> Message<ReceiptMeta, Quote> {
    Message::new(ReceiptMeta { order: order.id }, Quote { id: order.id })
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_order)
            .include(on_pair)
            .include(on_valve)
            .include(quote_order)
            .include(quote_pair)
            .include(quote_valve)
            .include(receipt)
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

/// Publishes `json` to `subject`, with the `tenant` header an order's contract reads.
async fn publish(bus: &Bus, subject: &'static str, json: &str) {
    let connected = bus.clone().connect().await.expect("the bus connects");
    let mut headers = HeaderMap::new();
    headers.insert("tenant", "acme");
    connected
        .publisher()
        .publish(
            OutgoingMessage::produced(subject, BytesMut::from(json.as_bytes()))
                .with_headers(headers),
            None,
        )
        .await
        .expect("the bus takes the message");
}

/// The next message `subscriber` holds, if it holds one now: its payload and its `order` header.
fn next<Sub: LocalSubscriber>(subscriber: &mut Sub) -> Option<(String, Option<Vec<u8>>)> {
    pin!(subscriber.stream())
        .next()
        .now_or_never()
        .flatten()
        .and_then(Result::ok)
        .map(|message| {
            (
                String::from_utf8_lossy(message.payload()).into_owned(),
                message.headers().get("order").map(<[u8]>::to_vec),
            )
        })
}

async fn check_the_forms(spawner: Spawner, stand: &'static Stand) {
    let connected = stand.bus.clone().connect().await.expect("the bus connects");
    let mut replies = Vec::new();
    for subject in ["out/order", "out/pair", "out/valve", "out/receipt"] {
        replies.push(
            LocalMemorySource::new(subject)
                .subscribe(&connected)
                .await
                .expect("the replies subscribe"),
        );
    }
    spawner.spawn(app::task(stand.bus.clone()).expect("the app task is free"));
    settle().await;

    publish(&stand.bus, "in/order", r#"{"id":1}"#).await;
    publish(&stand.bus, "in/pair", r#"{"id":2}"#).await;
    publish(&stand.bus, "in/valve", r#"{"closed":3}"#).await;
    publish(&stand.bus, "in/quote-order", r#"{"id":8}"#).await;
    publish(&stand.bus, "in/quote-pair", r#"{"id":9}"#).await;
    publish(&stand.bus, "in/quote-valve", r#"{"opened":10}"#).await;
    publish(&stand.bus, "in/receipt", r#"{"id":11}"#).await;
    settle().await;
    let answered: Vec<_> = replies.iter_mut().map(next).collect();

    app::CONTROL.stop();
    settle().await;

    assert_eq!(seen(0), 1, "the id the decoded handler saw");
    passed("a handler of a decoded value runs on the embedded app");
    assert_eq!(
        seen(1),
        2,
        "the id the pair handler saw under the acme tenant"
    );
    passed("a handler of a message pair reads its headers and its payload");
    assert_eq!(seen(2), 3, "the id the set handler saw");
    passed("a handler of a message set runs on the embedded app");
    assert_eq!(
        answered,
        [
            Some((r#"{"id":8}"#.to_owned(), None)),
            Some((r#"{"id":9}"#.to_owned(), None)),
            Some((r#"{"id":10}"#.to_owned(), None)),
            Some((r#"{"id":11}"#.to_owned(), Some(b"11".to_vec()))),
        ],
        "the replies of the decoded, pair and set handlers, and the receipt with its header"
    );
    passed("each form replies through the codec, a pair reply with its headers");
}

#[embassy_executor::task]
async fn drive(spawner: Spawner, stand: &'static Stand) {
    check_the_forms(spawner, stand).await;
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
