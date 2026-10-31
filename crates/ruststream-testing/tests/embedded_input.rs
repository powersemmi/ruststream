//! The input forms a device handler takes, tested on the host through `TestApp`: a value the
//! codec decodes, a `Message<Headers, Payload>` pair and a `#[derive(InMessages)]` set.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Message, Mounts};
use ruststream::testing::TestApp;
use ruststream::{InMessages, Outgoing, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A reading, decoded by the codec.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Reading {
    celsius: i32,
}

/// The header contract a reading travels with.
#[derive(Serialize, Deserialize)]
struct Origin {
    sensor: String,
}

/// A reading as the sensor sends it: the same payload, with its origin in the headers.
#[derive(Serialize, Outgoing)]
#[outgoing(headers = Origin)]
struct Tagged {
    celsius: i32,
}

/// A command for the valve: opened or closed, each decoded by the codec.
#[derive(Serialize, Deserialize, Outgoing)]
struct Opened {
    opened: u32,
}

/// See [`Opened`].
#[derive(Serialize, Deserialize, Outgoing)]
struct Closed {
    closed: u32,
}

/// The set a valve handler takes: the first model that decodes the payload.
#[derive(InMessages)]
enum Valve {
    Opened(Opened),
    Closed(Closed),
}

#[subscriber(LocalMemorySource::new("sensors/temp"))]
async fn on_reading(_reading: &Reading) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(LocalMemorySource::new("sensors/origin"))]
async fn on_origin(pair: &Message<Origin, Reading>) -> HandlerOutcome {
    if pair.headers.sensor == "boiler" {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

#[subscriber(LocalMemorySource::new("valve/command"))]
async fn on_valve(valve: &Valve) -> HandlerOutcome {
    let id = match valve {
        Valve::Opened(opened) => opened.opened,
        Valve::Closed(closed) => closed.closed,
    };
    if id == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_reading).include(on_origin).include(on_valve)
    })
}

#[test]
fn a_value_the_codec_decodes_reaches_its_handler() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Reading { celsius: 21 })
            .to("sensors/temp")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .assert_called_once()
            .with(&Reading { celsius: 21 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_message_pair_reaches_its_handler() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let origin = Origin {
            sensor: "boiler".into(),
        };
        tb.broker::<Bus>()
            .message(&Tagged { celsius: 80 })
            .with_headers(&origin)
            .to("sensors/origin")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("sensors/origin")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_message_set_reaches_its_handler() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Closed { closed: 7 })
            .to("valve/command")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("valve/command")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}
