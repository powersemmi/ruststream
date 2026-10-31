//! A device service's app tested on the host through `TestApp`, as a device crate tests it: the
//! handlers with `#[subscriber]`, the app with `#[ruststream::app]`, and the test handing the app
//! function its bus.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::future::pending;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use embassy_futures::{block_on, yield_now};
use embassy_time::Timer;
use ruststream::memory::local::prelude::{
    BytesMut, LocalBroker, LocalMemoryError, LocalMemoryPublisher, LocalPublisher, OutgoingMessage,
};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out, PublishError};
use ruststream::testing::{TestApp, TestError};
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A sensor's frame, as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A frame the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Logs every reading, holding a value that is not `Send` across an await.
#[subscriber(LocalMemorySource::new("sensors/temp"))]
async fn on_reading(reading: &Frame<'_>) -> HandlerOutcome {
    let held = Rc::new(reading.0.len());
    yield_now().await;
    if *held == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// An alert the level handler raises: the level's bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "alerts")]
struct Alert(Vec<u8>);

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Raises an alert for every level, holding a value that is not `Send` across the publish.
#[subscriber(LocalMemorySource::new("sensors/level"))]
async fn on_level(level: &Frame<'_>, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    let held = Rc::new(level.0.to_vec());
    match alerts.message(&Alert(held.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// Holds the valve open for ten seconds on the board's clock, then acknowledges.
#[subscriber(LocalMemorySource::new("valve/open"))]
async fn on_open(open: &Frame<'_>) -> HandlerOutcome {
    let _ = open.0;
    Timer::after_secs(10).await;
    HandlerOutcome::ack()
}

/// Waits on a signal no part of the board ever sends.
#[subscriber(LocalMemorySource::new("valve/stuck"))]
async fn on_stuck(open: &Frame<'_>) -> HandlerOutcome {
    let _ = open.0;
    pending::<()>().await;
    HandlerOutcome::ack()
}

/// Samples the valve's position every second until it reports closed, which this valve never
/// does.
#[subscriber(LocalMemorySource::new("valve/watch"))]
async fn on_watch(open: &Frame<'_>) -> HandlerOutcome {
    let _ = open.0;
    loop {
        Timer::after_secs(1).await;
    }
}

/// Spins on the valve without ever waiting on the clock.
#[subscriber(LocalMemorySource::new("valve/spin"))]
async fn on_spin(open: &Frame<'_>) -> HandlerOutcome {
    let _ = open.0;
    loop {
        yield_now().await;
    }
}

/// The board's bus, and a publisher on it that another task of the firmware holds: whether it
/// may still publish tells whether the bus was shut down.
async fn bus_and_sensor() -> Result<(Bus, LocalMemoryPublisher<4>), LocalMemoryError> {
    let bus = Bus::new();
    let sensor = bus.clone().connect().await?.publisher();
    Ok((bus, sensor))
}

/// Whether the bus refuses the sensor's publish because it was shut down.
async fn bus_is_shut_down(sensor: &LocalMemoryPublisher<4>) -> bool {
    let reading = OutgoingMessage::produced("sensors/temp", BytesMut::from("21"));
    sensor.publish(reading, None).await == Err(LocalMemoryError::ShutDown)
}

#[ruststream::app]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_level)
            .out(Alerts, LocalMemoryPublish)
            .include(on_reading)
            .include(on_open)
    })
}

#[test]
fn a_reading_reaches_its_handler() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .assert_called_once()
            .with_raw(b"21")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_out_publish_reaches_its_destination() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("sensors/level")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .published::<()>("alerts")
            .assert_called_once()
            .with_raw(b"high");
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_handler_waiting_on_the_clock_finishes_once_it_is_advanced() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"valve"))
            .to("valve/open")
            .publish()
            .await?;

        tb.advance(Duration::from_secs(9)).await?;
        tb.broker::<Bus>()
            .subscriber("valve/open")
            .assert_not_called();
        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<Bus>()
            .subscriber("valve/open")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_shutdown_lets_a_handler_waiting_on_the_clock_finish_and_shuts_the_bus_down()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let (bus, sensor) = bus_and_sensor().await?;
        let tb = TestApp::start(app(bus)).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"valve"))
            .to("valve/open")
            .publish()
            .await?;

        tb.shutdown().await?;
        assert!(
            bus_is_shut_down(&sensor).await,
            "the shutdown ended before the app shut its bus down"
        );
        Ok(())
    })
}

#[test]
fn a_shutdown_that_waits_on_nothing_the_clock_wakes_fails() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb =
            TestApp::start(EmbeddedApp::new().with_broker(Bus::new(), |b| b.include(on_stuck)))
                .await?;
        tb.broker::<Bus>()
            .message(&Sent(b"valve"))
            .to("valve/stuck")
            .publish()
            .await?;

        let stopped = tb.shutdown().await;
        assert!(
            matches!(stopped, Err(TestError::Stalled)),
            "a handler that never finishes keeps the app from stopping: {stopped:?}"
        );
        Ok(())
    })
}

#[test]
fn a_shutdown_of_a_handler_that_waits_on_the_clock_forever_fails() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb =
            TestApp::start(EmbeddedApp::new().with_broker(Bus::new(), |b| b.include(on_watch)))
                .await?;
        tb.broker::<Bus>()
            .message(&Sent(b"valve"))
            .to("valve/watch")
            .publish()
            .await?;

        let stopped = tb.shutdown().await;
        assert!(
            matches!(stopped, Err(TestError::NotQuiescent { .. })),
            "a handler that re-arms its timer forever keeps the app from stopping: {stopped:?}"
        );
        Ok(())
    })
}

#[test]
fn a_shutdown_of_a_handler_that_keeps_waking_itself_fails() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(EmbeddedApp::new().with_broker(Bus::new(), |b| b.include(on_spin)))
            .await?;
        let published = tb
            .broker::<Bus>()
            .message(&Sent(b"valve"))
            .to("valve/spin")
            .publish()
            .await;
        assert!(
            matches!(
                published,
                Err(PublishError::Publish(TestError::NotQuiescent { .. }))
            ),
            "the spinning handler keeps the publish from settling: {published:?}"
        );

        let stopped = tb.shutdown().await;
        assert!(
            matches!(stopped, Err(TestError::NotQuiescent { .. })),
            "the spinning handler keeps the app from stopping: {stopped:?}"
        );
        Ok(())
    })
}

#[test]
fn a_test_publish_after_the_app_stopped_is_refused() -> Result<(), Box<dyn Error>> {
    block_on(async {
        // An app with no route has nothing to run and ends once it started.
        let tb = TestApp::start(EmbeddedApp::new().with_broker(Bus::new(), |b| b)).await?;
        let refused = tb
            .broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await;
        assert!(
            refused.is_err(),
            "the publish reached a stopped app: {refused:?}"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_failing_after_startup_hook_fails_the_start() {
    let started = block_on(TestApp::start(
        EmbeddedApp::new()
            .with_broker(Bus::new(), |b| b.include(on_reading))
            .after_startup(async |_state| Err::<(), _>(io::Error::other("no calibration"))),
    ));
    let err = started.expect_err("the hook failed, so the start fails");
    assert!(
        matches!(err, TestError::Startup(_)),
        "a failed after_startup hook is a startup error: {err:?}"
    );
}

#[test]
#[should_panic(expected = "one TestApp runs at a time")]
fn a_second_harness_on_one_thread_is_refused() {
    block_on(async {
        let _first = TestApp::start(app(Bus::new())).await;
        let _second = TestApp::start(app(Bus::new())).await;
    });
}
