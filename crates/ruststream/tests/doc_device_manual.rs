//! The device service of the guide's chapter on the local memory broker, declared without the
//! macros and built without `alloc`: the message types write the trait impls the derives would,
//! and each subscriber is a named type with an `impl Handle`. The regions are the excerpts of the
//! chapter's Manual tabs, beside those of `doc_device.rs`.
//!
//! Run with `cargo test --no-default-features --features embassy,memory,macros,testing,postcard
//! --test doc_device_manual`.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::convert::Infallible;
use std::error::Error;
use std::future::ready;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::prelude::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    Context, EmbeddedApp, Handle, HandlerOutcome, MessageWire, MountSteps, Mounts, OutEntry,
    OutSlot, Outs, PublishedThrough, Reads, Serialized, SerializedWire, WireBytes, subscriber,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{EncodeBuf, FixedName, MessageHeaders, NoHeaders, OutgoingDestination, Publisher};
use serde::{Deserialize, Serialize};

// --8<-- [start:bus]
/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The bus is a `static` the board declares; the app function receives a broker built over it,
/// `Bus::new(&BUS)`.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
// --8<-- [end:bus]

// --8<-- [start:messages]
/// A sensor's reading, as the other end of the bus encodes it with postcard.
#[derive(Serialize, Deserialize)]
struct Reading {
    sensor: u8,
    millivolts: u16,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "sensors/temp")]` writes.
impl OutgoingDestination for Reading {
    type Form = FixedName;

    const DESTINATION: &'static str = "sensors/temp";
}

impl MessageHeaders for Reading {
    type Contract = NoHeaders;
}

/// What the board reports for a reading.
#[derive(Serialize, Deserialize)]
struct Report {
    sensor: u8,
    celsius: i16,
}

impl OutgoingDestination for Report {
    type Form = FixedName;

    const DESTINATION: &'static str = "reports";
}

impl MessageHeaders for Report {
    type Contract = NoHeaders;
}
// --8<-- [end:messages]

// --8<-- [start:reading-handler]
/// Reports every reading in degrees.
struct OnReading;

impl Handle<Reading, Report, (), (), ()> for OnReading {
    fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_, (), ()>,
    ) -> impl Future<Output = Result<Report, HandlerOutcome>> {
        let millivolts = i32::from(reading.millivolts);
        ready(Ok(Report {
            sensor: reading.sensor,
            celsius: i16::try_from((millivolts - 500) / 10).unwrap_or(i16::MAX),
        }))
    }
}
// --8<-- [end:reading-handler]

// --8<-- [start:reply-mount]
fn reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(
                subscriber(LocalMemorySource::new("sensors/temp"), OnReading)
                    .reply()
                    .build(),
            )
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .encode_capacity::<64>()
    })
}
// --8<-- [end:reply-mount]

// --8<-- [start:alarm]
/// The alert the board raises: the sensor's number, as its own byte.
struct Alert([u8; 1]);

// `#[derive(Outgoing, Serialized)]` with `#[outgoing(name = "alerts")]`, by hand.
impl OutgoingDestination for Alert {
    type Form = FixedName;

    const DESTINATION: &'static str = "alerts";
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
}

impl Serialized for Alert {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Alert {
    type Wire = SerializedWire;
}

/// The slot alerts leave through.
struct Alarms;

impl OutSlot for Alarms {
    const NAME: &'static str = "Alarms";
    type Destination = Reads;
}

impl PublishedThrough<Alarms> for Alert {}

/// Raises an alert for a report past fifty degrees.
struct OnHot;

impl<Entry> Handle<Report, (), Outs<(Entry,)>, (), ()> for OnHot
where
    Entry: OutEntry<Alarms, Wire: Publisher>,
{
    async fn handle(
        &self,
        report: &Report,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_, (), ()>,
    ) -> Result<(), HandlerOutcome> {
        if report.celsius <= 50 {
            return Ok(());
        }
        outs.get(Alarms)
            .message(&Alert([report.sensor]))
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:alarm]

// --8<-- [start:alarm-mount]
fn device_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(
                subscriber(LocalMemorySource::new("sensors/temp"), OnReading)
                    .reply()
                    .build(),
            )
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .encode_capacity::<64>()
            .include(subscriber(LocalMemorySource::new("reports"), OnHot).build())
            .out(Alarms, LocalMemoryPublish)
    })
}
// --8<-- [end:alarm-mount]

// --8<-- [start:allocator]
/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;
// --8<-- [end:allocator]

// --8<-- [start:device-test]
#[test]
fn a_hot_reading_raises_an_alert() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(device_app(Bus::new(&BUS))).await?;
        let reading = Reading {
            sensor: 2,
            millivolts: 1200,
        };
        tb.broker::<Bus>()
            .message(&reading)
            .with_codec(PostcardCodec)
            .publish()
            .await?;
        tb.broker::<Bus>()
            .subscriber("reports")
            .assert_called_once();
        tb.out::<Alarms>().assert_called_once().with_raw(&[2]);
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:device-test]

/// The bus of the mild scenario.
static MILD_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

#[test]
fn a_mild_reading_is_only_reported() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(reply_app(Bus::new(&MILD_BUS))).await?;
        let reading = Reading {
            sensor: 1,
            millivolts: 700,
        };
        tb.broker::<Bus>()
            .message(&reading)
            .with_codec(PostcardCodec)
            .publish()
            .await?;
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
