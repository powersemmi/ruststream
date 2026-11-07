//! The check of the no-alloc publish stand: the benchmark's own scenarios, each over a bus whose
//! destination has a reader, every message arriving. A binary of its own, so the benchmark's
//! holds the measured code alone.

// The app and its handlers are not `Send`: the check polls them inside its own future, which
// lives on the stack of the check, as a device keeps it.
#![allow(clippy::future_not_send, clippy::large_futures)]
#![cfg(not(ruststream_alloc))]

use std::cell::Cell;
use std::hint::black_box;

use embassy_futures::block_on;
use embassy_futures::join::join;
use embassy_futures::yield_now;
use ruststream::memory::local::{LocalMemoryBus, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{Control, EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::{Broker, OutgoingMessage, Publisher, subscriber};

use scenario::{Bus, Publish, Reading, Relay, answer, relay, relay_traced};

#[path = "../benches/embedded_publish_no_alloc/scenario.rs"]
mod scenario;

/// Deliveries a check sends.
const CHECKED: usize = 3;

thread_local! {
    /// Messages that reached the destination of the publish.
    static ARRIVED: Cell<usize> = const { Cell::new(0) };
}

fn arrived() {
    ARRIVED.with(|arrived| arrived.set(arrived.get() + 1));
}

/// The reader of the relayed readings.
#[subscriber(LocalMemorySource::new("relayed"))]
async fn relayed_arrival(reading: &Reading<'_>) -> HandlerOutcome {
    black_box(reading);
    arrived();
    HandlerOutcome::ack()
}

/// The reader of the answers.
#[subscriber(LocalMemorySource::new("answers"))]
async fn answer_arrival(reading: &Reading<'_>) -> HandlerOutcome {
    black_box(reading);
    arrived();
    HandlerOutcome::ack()
}

fn through_a_slot_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(relayed_arrival)
            .include(relay)
            .out(Relay, LocalMemoryPublish)
    })
}

fn with_a_header_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(relayed_arrival)
            .include(relay_traced)
            .out(Relay, LocalMemoryPublish)
    })
}

fn as_a_reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(answer_arrival)
            .include(answer)
            .out_reply(LocalMemoryPublish)
    })
}

/// Runs the scenario's handler over `bus`, whose destination has a reader, and fails unless each
/// delivery's message arrives there.
fn check(bus: Bus, publish: Publish) {
    let control = Control::new();
    block_on(async {
        let publisher = bus
            .connect()
            .await
            .expect("the local bus connects")
            .publisher();
        let drive = async {
            for _ in 0..4 {
                yield_now().await;
            }
            for done in 0..CHECKED {
                publisher
                    .publish(OutgoingMessage::with_payload("busy", &b"x"[..]), None)
                    .await
                    .expect("the bus takes the delivery");
                // A message that never arrives leaves the count short, and the run ends.
                for _ in 0..64 {
                    if ARRIVED.with(Cell::get) > done {
                        break;
                    }
                    yield_now().await;
                }
            }
            control.stop();
        };
        let run = async {
            match publish {
                Publish::ThroughASlot => through_a_slot_app(bus).run(&control).await,
                Publish::WithAHeader => with_a_header_app(bus).run(&control).await,
                Publish::AsAReply => as_a_reply_app(bus).run(&control).await,
            }
        };
        let (ran, ()) = join(run, drive).await;
        ran.expect("the app stops cleanly");
    });
    assert_eq!(
        ARRIVED.with(Cell::get),
        CHECKED,
        "each delivery's message arrived at its destination"
    );
}

#[test]
fn a_slot_publish_arrives() {
    static BUS: LocalMemoryBus<4, 64, 16> = LocalMemoryBus::new();
    check(Bus::new(&BUS), Publish::ThroughASlot);
}

#[test]
fn a_slot_publish_with_a_header_arrives() {
    static BUS: LocalMemoryBus<4, 64, 16> = LocalMemoryBus::new();
    check(Bus::new(&BUS), Publish::WithAHeader);
}

#[test]
fn a_reply_arrives() {
    static BUS: LocalMemoryBus<4, 64, 16> = LocalMemoryBus::new();
    check(Bus::new(&BUS), Publish::AsAReply);
}
