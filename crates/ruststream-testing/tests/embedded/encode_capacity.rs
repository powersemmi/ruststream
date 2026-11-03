//! A device service that names the room its replies are encoded into: the mount
//! keeps the count, with a reply publisher or without one, and a reply reaches the bus as the
//! default codec wrote it.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use serde::Serialize;

/// The board's bus.
type Bus = LocalMemoryBroker<4>;

/// An ask, as its bytes.
#[derive(Deserialized)]
struct Ask<'a>(&'a [u8]);

/// A message the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The answer, which the default codec writes.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Verdict {
    ok: bool,
}

/// Answers every ask.
#[subscriber(LocalMemorySource::new("ask"), reply)]
async fn on_ask(ask: &Ask<'_>) -> Verdict {
    let _ = ask.0;
    Verdict { ok: true }
}

#[ruststream::app(runtime = embassy)]
fn reply_then_room(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_ask)
            .out_reply(LocalMemoryPublish)
            .encode_capacity::<64>()
    })
}

/// Takes a job and answers nothing.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Ask<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn room_without_reply(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).encode_capacity::<64>())
}

async fn answer_to_an_ask(app: EmbeddedApp<impl Mounts + 'static>) -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app).await?;
    tb.broker::<Bus>()
        .message(&Sent(b"?"))
        .to("ask")
        .publish()
        .await?;

    tb.broker::<Bus>()
        .published::<()>("answers")
        .assert_called_once()
        .with_raw(br#"{"ok":true}"#);
    tb.shutdown().await?;
    Ok(())
}

#[test]
fn a_reply_publisher_attached_before_the_room_encodes_into_it() -> Result<(), Box<dyn Error>> {
    block_on(answer_to_an_ask(reply_then_room(Bus::new())))
}

#[test]
fn a_room_named_on_a_handler_without_a_reply_leaves_it_running() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(room_without_reply(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"job"))
            .to("jobs")
            .publish()
            .await?;

        tb.broker::<Bus>().subscriber("jobs").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
