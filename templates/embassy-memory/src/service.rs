//! The device service: messages and handlers, written as `#[subscriber]` functions, and the app.
//!
//! `on_reading` takes a sensor's readings from the bus and replies with a report in degrees;
//! `on_report` takes the reports and logs each once it is acknowledged.

use ruststream::codec::PostcardCodec;
use ruststream::diagnostics::DefmtLogger;
use ruststream::memory::local::{LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

use crate::Bus;

/// A sensor's reading, as the other end of the bus encodes it with postcard.
#[derive(Deserialize)]
pub struct Reading {
    pub sensor: u8,
    pub millivolts: u16,
}

/// What the board reports for a reading.
///
/// `Outgoing` is what declares it a message this service sends, and `name` is where it goes.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "reports")]
pub struct Report {
    pub sensor: u8,
    pub celsius: i16,
}

/// Reports every reading in degrees: the return value is the reply.
#[subscriber(LocalMemorySource::new("sensors/temp"), reply)]
async fn on_reading(reading: &Reading) -> Report {
    let millivolts = i32::from(reading.millivolts);
    Report {
        sensor: reading.sensor,
        celsius: i16::try_from((millivolts - 500) / 10).unwrap_or(i16::MAX),
    }
}

/// Takes a report and logs it once the report is acknowledged.
#[subscriber(LocalMemorySource::new("reports"))]
async fn on_report(report: &Report) -> HandlerOutcome {
    let (sensor, celsius) = (report.sensor, report.celsius);
    // The continuation lives in a room of 128 bytes; a capture past it does not compile.
    HandlerOutcome::ack().and_after(async move { defmt::info!("sensor {}: {} C", sensor, celsius) })
}

/// Builds the app over the board's bus. `#[ruststream::app]` makes it the embassy task
/// `app::task`, which `main` spawns.
///
/// Both handlers decode with postcard; the reply encodes with it too, into a buffer of 64 bytes
/// the mount fixes, so a report never reaches for a heap.
#[ruststream::app(runtime = embassy)]
pub fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new()
        .logger(DefmtLogger)
        .with_broker(bus, |b| {
            b.with_codec(PostcardCodec)
                .include(on_report)
                .include(on_reading)
                .out_reply(LocalMemoryPublish)
                .codec(PostcardCodec)
                .encode_capacity::<64>()
        })
}
