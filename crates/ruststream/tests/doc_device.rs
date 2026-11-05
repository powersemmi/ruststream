//! The device service the embedded runtime page teaches, built without `alloc`: a handler on the
//! board's bus, a reply, a retry, a continuation and a batch, each in the form the build holds.
//! The regions are the excerpts of that page.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use assert_no_alloc::AllocDisabler;
use embassy_time::Timer;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, Out, Outcomes, SubscriberSettings,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use serde::{Deserialize, Serialize};

// --8<-- [start:bus]
/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The bus is a `static` the board declares; the app function receives a broker built over it,
/// `Bus::new(&BUS)`.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
// --8<-- [end:bus]

// --8<-- [start:service]

// --8<-- [start:messages]
/// A sensor's reading, as the other end of the bus encodes it with postcard.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    sensor: u8,
    millivolts: u16,
}

/// What the board reports for a reading.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "reports")]
struct Report {
    sensor: u8,
    celsius: i16,
}
// --8<-- [end:messages]

// --8<-- [start:reading-handler]
/// Reports every reading in degrees.
#[subscriber(LocalMemorySource::new("sensors/temp"), reply)]
async fn on_reading(reading: &Reading) -> Report {
    let millivolts = i32::from(reading.millivolts);
    Report {
        sensor: reading.sensor,
        celsius: i16::try_from((millivolts - 500) / 10).unwrap_or(i16::MAX),
    }
}
// --8<-- [end:reading-handler]

/// Takes the reports and updates the display once each is acknowledged.
#[subscriber(LocalMemorySource::new("reports"))]
async fn on_report(report: &Report) -> HandlerOutcome {
    let celsius = report.celsius;
    // The continuation lives in a room of 128 bytes; a capture past it does not compile.
    HandlerOutcome::ack().and_after(async move { show(celsius).await })
}

/// Draws a temperature on the board's display.
async fn show(celsius: i16) {
    Timer::after_millis(10).await;
    let _ = celsius;
}

// --8<-- [start:mount]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(on_report)
            .include(on_reading)
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .encode_capacity::<64>()
    })
}
// --8<-- [end:mount]
// --8<-- [end:service]

/// The retries that found no room to wait, in a counter of the board's.
static LOST: AtomicUsize = AtomicUsize::new(0);

/// The state of the app: the counter a handler reaches.
#[derive(Clone, Copy)]
struct Lost(&'static AtomicUsize);

// --8<-- [start:allocator]
/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;
// --8<-- [end:allocator]

// --8<-- [start:reply-mount]
fn reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(on_reading)
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .encode_capacity::<64>()
    })
}
// --8<-- [end:reply-mount]

// --8<-- [start:alarm]
/// The alert the board raises: the sensor's number, as its own byte.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "alerts")]
struct Alert([u8; 1]);

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alarms;

/// Raises an alert for a report past fifty degrees.
#[subscriber(LocalMemorySource::new("reports"))]
async fn on_hot(report: &Report, Out(alarms): Out<impl Publisher, Alarms>) -> HandlerOutcome {
    if report.celsius <= 50 {
        return HandlerOutcome::ack();
    }
    match alarms.message(&Alert([report.sensor])).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:alarm]

// --8<-- [start:alarm-mount]
fn device_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(on_reading)
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .encode_capacity::<64>()
            .include(on_hot)
            .out(Alarms, LocalMemoryPublish)
    })
}
// --8<-- [end:alarm-mount]

// --8<-- [start:retry]
/// A humidity reading.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "sensors/humidity")]
struct Humidity {
    sensor: u8,
    percent: u8,
}

/// Asks for the reading again in a minute, and counts a retry that found no room to wait.
#[subscriber(LocalMemorySource::new("sensors/humidity"))]
async fn on_humidity(reading: &Humidity, ctx: &mut Context<'_, (), Lost>) -> HandlerOutcome {
    let lost = ctx.state().0;
    let _ = reading.sensor;
    HandlerOutcome::retry_after(Duration::from_secs(60)).error(async move |failures: &Failures| {
        lost.fetch_add(failures.len(), Ordering::Relaxed);
    })
}

fn retrying_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost(&LOST))
        .with_broker(bus, |b| {
            // Room for two delayed retries at a time.
            b.with_codec(PostcardCodec)
                .include(on_humidity)
                .retry_capacity::<2>()
        })
}
// --8<-- [end:retry]

// --8<-- [start:batch]
/// A frame from the sensor, as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// Takes the frames that arrived together, up to eight.
#[subscriber(LocalMemorySource::new("sensors/burst"))]
async fn on_burst(frames: &[Frame<'_>]) -> HandlerOutcome {
    let bytes: usize = frames.iter().map(|frame| frame.0.len()).sum();
    let _ = bytes;
    HandlerOutcome::ack()
}

fn batching_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_burst.batch::<8>()))
}
// --8<-- [end:batch]

// --8<-- [start:batch-each]
/// Settles each frame on its own: an empty frame is dropped, the others are acknowledged.
#[subscriber(LocalMemorySource::new("sensors/frames"))]
async fn on_frames(frames: &[Frame<'_>]) -> Outcomes<HandlerOutcome, 4> {
    let mut outcomes = Outcomes::new();
    for frame in frames {
        let outcome = if frame.0.is_empty() {
            HandlerOutcome::drop()
        } else {
            HandlerOutcome::ack()
        };
        // The mount takes at most four frames, so every outcome has its place.
        let _ = outcomes.push(outcome);
    }
    outcomes
}

fn framing_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_frames.batch::<4>()))
}
// --8<-- [end:batch-each]

// --8<-- [start:batch-retry]
/// A reading that may be a spike.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "sensors/spikes")]
struct Spike {
    sensor: u8,
    millivolts: u16,
}

/// Asks for the whole burst again in a minute when the display is busy.
#[subscriber(LocalMemorySource::new("sensors/spikes"))]
async fn on_spikes(readings: &[Spike]) -> Outcomes<HandlerOutcome, 4> {
    readings
        .iter()
        .map(|reading| {
            if reading.millivolts > 3000 {
                HandlerOutcome::retry_after(Duration::from_secs(60))
            } else {
                HandlerOutcome::ack()
            }
        })
        .collect()
}

fn spiking_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        // Room for the four delayed copies a full batch can ask for.
        b.with_codec(PostcardCodec)
            .include(on_spikes.batch::<4>())
            .retry_capacity::<4>()
    })
}
// --8<-- [end:batch-retry]

// --8<-- [start:batch-reply]
/// A reading of the burst.
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "sensors/batch")]
struct Sample {
    sensor: u8,
    millivolts: u16,
}

/// Reports every reading of the batch, one report for each.
#[subscriber(LocalMemorySource::new("sensors/batch"), reply)]
async fn on_readings(readings: &[Sample]) -> Outcomes<Report, 4> {
    readings
        .iter()
        .map(|reading| Report {
            sensor: reading.sensor,
            celsius: i16::try_from((i32::from(reading.millivolts) - 500) / 10).unwrap_or(i16::MAX),
        })
        .collect()
}

fn reporting_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(on_report)
            .include(on_readings.batch::<4>())
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
    })
}
// --8<-- [end:batch-reply]

/// A frame the test publishes to the per-frame subscription.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "sensors/frames")]
struct Framed(&'static [u8]);

#[test]
fn a_batch_settles_each_frame() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(framing_app(Bus::new(&FRAMES_BUS))).await?;
        tb.broker::<Bus>().message(&Framed(b"")).publish().await?;
        tb.settle().await?;
        tb.broker::<Bus>()
            .subscriber("sensors/frames")
            .settled(HandlerOutcome::drop());
        tb.broker::<Bus>().message(&Framed(b"ok")).publish().await?;
        tb.settle().await?;
        tb.broker::<Bus>()
            .subscriber("sensors/frames")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

/// The bus of the per-frame scenario.
static FRAMES_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// A frame the test publishes to the burst subscription.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "sensors/burst")]
struct Burst(&'static [u8]);

#[test]
fn a_reading_is_reported() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new(&BUS))).await?;
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
            .subscriber("reports")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

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

/// The bus of the mild scenario.
static MILD_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

// --8<-- [start:test]
#[test]
fn a_retry_waits_for_its_delay() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(retrying_app(Bus::new(&BUS))).await?;
        let reading = Humidity {
            sensor: 1,
            percent: 40,
        };
        tb.broker::<Bus>()
            .message(&reading)
            .with_codec(PostcardCodec)
            .publish()
            .await?;
        tb.advance(Duration::from_secs(60)).await?;
        tb.broker::<Bus>()
            .subscriber("sensors/humidity")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:test]

#[test]
fn a_burst_arrives_as_a_batch() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(batching_app(Bus::new(&BUS))).await?;
        tb.broker::<Bus>().message(&Burst(b"21")).publish().await?;
        tb.broker::<Bus>()
            .subscriber("sensors/burst")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

/// The stack a batch scenario is held to: the build without optimisation lays every frame of the
/// settling out in full.
const STACK: usize = 8 * 1024 * 1024;

/// Runs a scenario on a thread of the stack [`STACK`].
fn on_big_stack(
    scenario: impl AsyncFnOnce() -> Result<(), Box<dyn Error>> + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .stack_size(STACK)
        .spawn(move || block_on(scenario()).map_err(|err| err.to_string()))?
        .join()
        .map_err(|_| "the batch scenario panicked")??;
    Ok(())
}

/// The bus of the batch retry scenario.
static SPIKES_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// The bus of the batch reply scenario.
static BATCH_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

#[test]
fn a_batch_retries_its_elements() -> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let tb = TestApp::start(spiking_app(Bus::new(&SPIKES_BUS))).await?;
        let spike = Spike {
            sensor: 1,
            millivolts: 3300,
        };
        tb.broker::<Bus>()
            .message(&spike)
            .with_codec(PostcardCodec)
            .publish()
            .await?;
        tb.advance(Duration::from_secs(60)).await?;
        tb.broker::<Bus>()
            .subscriber("sensors/spikes")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_batch_replies_to_each_reading() -> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let tb = TestApp::start(reporting_app(Bus::new(&BATCH_BUS))).await?;
        let reading = Sample {
            sensor: 1,
            millivolts: 700,
        };
        tb.broker::<Bus>()
            .message(&reading)
            .with_codec(PostcardCodec)
            .publish()
            .await?;
        tb.broker::<Bus>()
            .subscriber("reports")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
