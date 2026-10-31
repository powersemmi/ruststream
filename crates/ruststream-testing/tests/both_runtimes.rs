//! One service, two runners: the same `#[subscriber]` handlers mount on a tokio app and on an
//! embedded app, in one build with both runtime features on.

// The embedded app and the handler that holds an `Rc` are not `Send`: the embedded harness polls
// them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::rc::Rc;

use embassy_futures::{block_on, yield_now};
use ruststream::memory::MemoryBroker;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish};
use ruststream::memory::prelude::Publish;
use ruststream::runtime::{AppInfo, EmbeddedApp, HandlerOutcome, Mounts, Out, RustStream};
use ruststream::testing::TestApp;
use ruststream::testing::embedded::TestApp as EmbeddedTestApp;
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A level reading, as JSON.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Reading {
    level: u32,
}

/// The alert a high reading raises.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    level: u32,
}

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Raises an alert for a reading over ten.
#[subscriber("readings")]
async fn on_reading(reading: &Reading, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    if reading.level > 10
        && alerts
            .message(&Alert {
                level: reading.level,
            })
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Acknowledges every reading it audits.
#[subscriber("audit")]
async fn on_audit(reading: &Reading) -> HandlerOutcome {
    let _ = reading.level;
    HandlerOutcome::ack()
}

/// Holds a value that is not `Send` across an await: it mounts on the embedded app only.
#[subscriber("local")]
async fn on_local(reading: &Reading) -> HandlerOutcome {
    let held = Rc::new(reading.level);
    yield_now().await;
    if *held == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The server's app, on tokio.
fn server() -> RustStream {
    RustStream::new(AppInfo::new("levels", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(on_reading).out(Alerts, Publish).include(on_audit)
    })
}

/// The device's app, on embassy: the same handlers, and one that only a device runs.
#[ruststream::app(runtime = embassy)]
fn device(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_reading)
            .out(Alerts, LocalMemoryPublish)
            .include(on_audit)
            .include(on_local)
    })
}

#[tokio::test]
async fn the_server_runs_the_shared_handlers() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(server()).await?;
    let broker = tb.broker::<MemoryBroker>();
    broker
        .message(&Reading { level: 42 })
        .to("readings")
        .publish()
        .await?;
    broker
        .message(&Reading { level: 3 })
        .to("audit")
        .publish()
        .await?;

    broker
        .subscriber("readings")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
    broker.subscriber("audit").assert_called(1);
    broker.published::<()>("alerts").assert_called(1);
    Ok(())
}

#[test]
fn the_device_runs_the_same_handlers() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = EmbeddedTestApp::start(device(Bus::new())).await?;
        let bus = tb.broker::<Bus>();
        bus.message(&Reading { level: 42 })
            .to("readings")
            .publish()
            .await?;
        bus.message(&Reading { level: 3 })
            .to("audit")
            .publish()
            .await?;
        bus.message(&Reading { level: 7 })
            .to("local")
            .publish()
            .await?;

        bus.subscriber("readings")
            .assert_called(1)
            .settled(HandlerOutcome::ack());
        bus.subscriber("audit").assert_called(1);
        bus.subscriber("local")
            .assert_called(1)
            .settled(HandlerOutcome::ack());
        bus.published::<()>("alerts").assert_called(1);
        tb.shutdown().await?;
        Ok(())
    })
}
