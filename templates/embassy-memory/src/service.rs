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
///
/// `Serialize` and `Outgoing` are what a test needs to send a reading through the bus.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use assert_no_alloc::AllocDisabler;
    use defmt_rtt as _;
    use embassy_futures::block_on;
    use ruststream::codec::PostcardCodec;
    use ruststream::memory::local::LocalMemoryBus;
    use ruststream::runtime::HandlerOutcome;
    use ruststream::testing::embedded::TestApp;

    use super::{Reading, app};
    use crate::Bus;

    /// Fails an allocation the framework makes while the harness polls the app. The harness's own
    /// bookkeeping runs where the allocator lets it through.
    #[global_allocator]
    static ALLOCATOR: AllocDisabler = AllocDisabler;

    // The firmware's linker script supplies defmt's default timestamp; the host has none.
    defmt::timestamp!("{=u64}", 0);

    /// The board declares its bus as a `static`; each test has one of its own.
    static HANDLER_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
    static REPLY_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

    /// 700 mV is 20 degrees: the sensor reads 500 mV at zero and 10 mV for each degree.
    const WARM: Reading = Reading {
        sensor: 1,
        millivolts: 700,
    };

    /// The test starts the app the board runs and sends a reading through its bus.
    #[test]
    fn a_reading_is_handled_and_acknowledged() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let tb = TestApp::start(app(Bus::new(&HANDLER_BUS))).await?;
            tb.broker::<Bus>()
                .message(&WARM)
                .with_codec(PostcardCodec)
                .publish()
                .await?;

            tb.broker::<Bus>()
                .subscriber("sensors/temp")
                .assert_called_once()
                .settled(HandlerOutcome::ack());
            tb.shutdown().await?;
            Ok(())
        })
    }

    /// The reply is recorded on its destination, as the postcard bytes of the report: sensor 1,
    /// then 20 degrees as a zigzag varint.
    #[test]
    fn a_reading_is_reported_in_degrees() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let tb = TestApp::start(app(Bus::new(&REPLY_BUS))).await?;
            tb.broker::<Bus>()
                .message(&WARM)
                .with_codec(PostcardCodec)
                .publish()
                .await?;

            tb.broker::<Bus>()
                .published::<()>("reports")
                .assert_called_once()
                .with_raw(&[1, 40]);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
