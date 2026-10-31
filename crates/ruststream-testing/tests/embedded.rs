//! A device service's app tested on the host through `TestApp`, as a device crate tests it: the
//! handlers with `#[subscriber]`, the app with `#[ruststream::app]`, and the test handing the app
//! function its bus.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::future::pending;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use embassy_futures::{block_on, yield_now};
use embassy_time::Timer;
use ruststream::memory::local::prelude::{
    BytesMut, ConnectedLocalMemoryBroker, LocalBroker, LocalMemoryBroker, LocalMemoryError,
    LocalMemoryPublish, LocalMemoryPublisher, LocalMemorySource, LocalPublisher, OutgoingMessage,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out, PublishError};
use ruststream::testing::LocalInProcess;
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

/// A level the board is set to, one byte on the bus.
#[derive(Deserialized)]
#[wire(decode = read_setting)]
struct Setting {
    level: u8,
}

/// Reads a setting off its one byte.
fn read_setting(payload: &[u8]) -> Result<Setting, io::Error> {
    match payload {
        [level] => Ok(Setting { level: *level }),
        _ => Err(io::Error::other("a setting is one byte")),
    }
}

/// Applies a setting; one that does not decode stops the app.
#[subscriber(LocalMemorySource::new("settings"), on_failure(decode = fail_fast))]
async fn on_setting(setting: &Setting) -> HandlerOutcome {
    let _ = setting.level;
    HandlerOutcome::ack()
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
fn a_fail_fast_stop_set_off_by_the_test_shuts_the_bus_down() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let (bus, sensor) = bus_and_sensor().await?;
        let tb =
            TestApp::start(EmbeddedApp::new().with_broker(bus, |b| b.include(on_setting))).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"twelve"))
            .to("settings")
            .publish()
            .await?;

        assert!(
            bus_is_shut_down(&sensor).await,
            "the app stopped on the failure and left its bus open"
        );
        let stopped = tb.shutdown().await;
        assert!(
            matches!(stopped, Err(TestError::Failed(_))),
            "the app stopped on its subscription's failure: {stopped:?}"
        );
        Ok(())
    })
}

#[test]
fn a_destination_hook_runs_for_the_tests_publish_that_stops_the_app() -> Result<(), Box<dyn Error>>
{
    block_on(async {
        let tb =
            TestApp::start(EmbeddedApp::new().with_broker(Bus::new(), |b| b.include(on_setting)))
                .await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .published::<()>("settings")
            .on_publish(async move |settings| {
                settings.assert_called_once().with_raw(b"twelve");
                seen.fetch_add(1, Ordering::SeqCst);
            });

        tb.broker::<Bus>()
            .message(&Sent(b"twelve"))
            .to("settings")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        let stopped = tb.shutdown().await;
        assert!(
            matches!(stopped, Err(TestError::Failed(_))),
            "the app stopped on its subscription's failure: {stopped:?}"
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

// --8<-- [start:out]
#[test]
fn an_out_slot_publish_is_recorded_with_its_slot() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("sensors/level")
            .publish()
            .await?;

        tb.out::<Alerts>().assert_called_once().with_raw(b"high");
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:out]

#[test]
fn a_call_hook_runs_in_the_publish_that_caused_the_call() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .on_call(async move |readings| {
                readings
                    .assert_called(seen.fetch_add(1, Ordering::SeqCst) + 1)
                    .settled(HandlerOutcome::ack());
            });

        for reading in [Sent(b"21"), Sent(b"22")] {
            tb.broker::<Bus>()
                .message(&reading)
                .to("sensors/temp")
                .publish()
                .await?;
        }
        assert_eq!(hooked.load(Ordering::SeqCst), 2);
        tb.shutdown().await?;
        Ok(())
    })
}

// --8<-- [start:on-publish]
#[test]
fn a_slot_publish_hook_runs_in_the_publish_that_caused_it() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.out::<Alerts>().on_publish(async move |alerts| {
            alerts.assert_called_once().with_raw(b"high");
            seen.fetch_add(1, Ordering::SeqCst);
        });

        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("sensors/level")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:on-publish]

#[test]
fn a_hook_waiting_on_the_clock_finishes_once_it_is_advanced() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .on_call(async move |_| {
                Timer::after_secs(1).await;
                seen.fetch_add(1, Ordering::SeqCst);
            });

        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 0);
        tb.advance(Duration::from_secs(1)).await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
#[should_panic(expected = "the reading failed its check")]
fn a_failed_hook_fails_the_publish_that_ran_it() {
    let _ = block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .on_call(async move |_| panic!("the reading failed its check"));

        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await?;
        Ok::<_, Box<dyn Error>>(tb)
    });
}

#[test]
#[should_panic(expected = "the alert failed its check")]
fn a_failed_hook_fails_the_advance_that_ran_it() {
    let _ = block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .subscriber("valve/open")
            .on_call(async move |_| panic!("the alert failed its check"));
        tb.broker::<Bus>()
            .message(&Sent(b"valve"))
            .to("valve/open")
            .publish()
            .await?;

        tb.advance(Duration::from_secs(10)).await?;
        Ok::<_, Box<dyn Error>>(tb)
    });
}

#[test]
fn a_destination_hook_runs_for_the_apps_slot_publish() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .published::<()>("alerts")
            .on_publish(async move |alerts| {
                alerts.assert_called_once().with_raw(b"high");
                seen.fetch_add(1, Ordering::SeqCst);
            });

        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("sensors/level")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_destination_hook_runs_for_the_tests_own_publish() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .published::<()>("sensors/temp")
            .on_publish(async move |readings| {
                readings.assert_called_once().with_raw(b"21");
                seen.fetch_add(1, Ordering::SeqCst);
            });

        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_destination_hook_waiting_on_the_clock_leaves_the_publish_and_finishes_on_advance()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .published::<()>("sensors/temp")
            .on_publish(async move |_readings| {
                Timer::after_secs(1).await;
                seen.fetch_add(1, Ordering::SeqCst);
            });

        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await?;
        assert_eq!(
            hooked.load(Ordering::SeqCst),
            0,
            "the hook waits for the clock, and the publish does not wait for it"
        );
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        tb.advance(Duration::from_secs(1)).await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_shutdown_lets_a_hook_waiting_on_the_clock_finish_and_shuts_the_bus_down()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let (bus, sensor) = bus_and_sensor().await?;
        let tb = TestApp::start(app(bus)).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .subscriber("sensors/temp")
            .on_call(async move |_| {
                Timer::after_secs(1).await;
                seen.fetch_add(1, Ordering::SeqCst);
            });
        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("sensors/temp")
            .publish()
            .await?;

        tb.shutdown().await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1, "the hook ran to its end");
        assert!(
            bus_is_shut_down(&sensor).await,
            "the shutdown ended before the app shut its bus down"
        );
        Ok(())
    })
}

/// What the board's handlers read: the level from which a reading is an alarm.
struct Limits {
    alarm: u8,
    /// Where the board echoes each alarm, when something listens.
    echo: Option<LocalMemoryPublisher<4>>,
}

/// Raises an alarm for every level at or above the board's limit.
#[subscriber(LocalMemorySource::new("tank/level"))]
async fn on_tank(
    level: &Frame<'_>,
    ctx: &mut Context<'_, (), Limits>,
    Out(alerts): Out<impl Publisher, Alerts>,
) -> HandlerOutcome {
    let limits = ctx.state();
    if level.0.first().copied().unwrap_or(0) < limits.alarm {
        return HandlerOutcome::ack();
    }
    if alerts
        .message(&Alert(level.0.to_vec()))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    if let Some(echo) = &limits.echo {
        let echoed = OutgoingMessage::produced("tank/echo", BytesMut::from(level.0));
        if echo.publish(echoed, None).await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

/// The tank's app, with the limits the board reads from its flash.
#[ruststream::app]
fn tank(bus: Bus) -> EmbeddedApp<impl Mounts<State = Limits>> {
    EmbeddedApp::new()
        .with_state(Limits {
            alarm: 200,
            echo: None,
        })
        .with_broker(bus, |b| b.include(on_tank).out(Alerts, LocalMemoryPublish))
}

// --8<-- [start:with-state]
#[test]
fn the_test_hands_the_app_its_state() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::with_state(tank(Bus::new()), |brokers| Limits {
            alarm: 50,
            echo: Some(brokers.broker::<Bus>().publisher()),
        })
        .await?;
        tb.broker::<Bus>()
            .message(&Sent(b"\x64"))
            .to("tank/level")
            .publish()
            .await?;

        tb.out::<Alerts>().assert_called_once().with_raw(b"\x64");
        tb.broker::<Bus>()
            .published::<()>("tank/echo")
            .assert_called_once()
            .with_raw(b"\x64");
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:with-state]

#[test]
fn without_a_state_of_its_own_the_test_runs_the_apps() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(tank(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"\x64"))
            .to("tank/level")
            .publish()
            .await?;

        tb.out::<Alerts>().assert_not_called();
        tb.shutdown().await?;
        Ok(())
    })
}

/// How a [`SlowBus`] ends its connect.
#[derive(Clone, Copy)]
enum Link {
    /// Connects after the board's radio has had a second.
    Ready,
    /// Refuses after a second.
    Refused,
    /// Never answers.
    Silent,
    /// Keeps retrying on a timer and never connects.
    Retrying,
}

/// A bus whose connection takes time on the clock, as a radio link does.
struct SlowBus(Link);

impl LocalBroker for SlowBus {
    type Error = LocalMemoryError;
    type Connected = ConnectedLocalMemoryBroker<4>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Bus::new().connect().await
    }
}

impl LocalInProcess for SlowBus {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        match self.0 {
            Link::Silent => pending().await,
            Link::Retrying => loop {
                Timer::after_secs(1).await;
            },
            Link::Ready | Link::Refused => Timer::after_secs(1).await,
        }
        match self.0 {
            Link::Refused => Err(LocalMemoryError::ShutDown),
            Link::Ready | Link::Silent | Link::Retrying => Bus::new().connect().await,
        }
    }
}

#[test]
fn a_start_waits_for_a_broker_that_connects_on_the_clock() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb =
            TestApp::start(EmbeddedApp::new().with_broker(SlowBus(Link::Ready), |b| b)).await?;
        // The broker is addressable only once the start has connected it.
        let _ = tb.broker::<SlowBus>();
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_broker_that_refuses_after_waiting_on_the_clock_fails_the_start() {
    let started = block_on(TestApp::start(
        EmbeddedApp::new().with_broker(SlowBus(Link::Refused), |b| b),
    ));
    let err = started.expect_err("the connection was refused, so the start fails");
    assert!(
        matches!(err, TestError::Connect { .. }),
        "a refused connection is a connect error: {err:?}"
    );
}

#[test]
fn a_start_that_waits_on_nothing_the_clock_wakes_fails() {
    let started = block_on(TestApp::start(
        EmbeddedApp::new().with_broker(SlowBus(Link::Silent), |b| b),
    ));
    let err = started.expect_err("the connection never answers, so the start fails");
    assert!(
        matches!(err, TestError::Stalled),
        "a connection nothing wakes is a stalled start: {err:?}"
    );
}

#[test]
fn a_start_that_keeps_retrying_on_the_clock_fails() {
    let started = block_on(TestApp::start(
        EmbeddedApp::new().with_broker(SlowBus(Link::Retrying), |b| b),
    ));
    let err = started.expect_err("the connection never ends, so the start fails");
    assert!(
        matches!(err, TestError::NotQuiescent { .. }),
        "a connection that re-arms its timer forever is not quiescent: {err:?}"
    );
}
