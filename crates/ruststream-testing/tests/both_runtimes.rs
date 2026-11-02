//! One service, two runners: the same `#[subscriber]` handlers mount on a tokio app and on an
//! embedded app, in one build with both runtime features on.

// The embedded app is not `Send`: the embedded harness polls it inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::{block_on, yield_now};
use ruststream::memory::MemoryBroker;
use ruststream::memory::local::LocalMemoryBroker;
use ruststream::runtime::{AppInfo, EmbeddedApp, HandlerOutcome, Mounts, RustStream};
use ruststream::testing::TestApp;
use ruststream::testing::embedded::TestApp as EmbeddedTestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A level reading, as its bytes. The input is a `Deserialized` view: decoding a `serde` input on
/// the embedded runner waits on the local message trait folding into the core one.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// A reading the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Acknowledges every reading it takes.
#[subscriber("readings")]
async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

/// Acknowledges every reading it audits.
#[subscriber("audit")]
async fn on_audit(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

/// Holds a value across an await: it mounts on the embedded app.
#[subscriber("local")]
async fn on_local(reading: &Reading<'_>) -> HandlerOutcome {
    let held = reading.0.len();
    yield_now().await;
    if held == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The server's app, on tokio.
fn server() -> RustStream {
    RustStream::new(AppInfo::new("levels", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(on_reading).include(on_audit)
    })
}

/// The device's app, on embassy: the same handlers, and one that only a device runs.
#[ruststream::app(runtime = embassy)]
fn device(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_reading).include(on_audit).include(on_local)
    })
}

#[tokio::test]
async fn the_server_runs_the_shared_handlers() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(server()).await?;
    let broker = tb.broker::<MemoryBroker>();
    broker
        .message(&Sent(b"42"))
        .to("readings")
        .publish()
        .await?;
    broker.message(&Sent(b"3")).to("audit").publish().await?;

    broker
        .subscriber("readings")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
    broker.subscriber("audit").assert_called(1);
    Ok(())
}

#[test]
fn the_device_runs_the_same_handlers() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = EmbeddedTestApp::start(device(Bus::new())).await?;
        let bus = tb.broker::<Bus>();
        bus.message(&Sent(b"42")).to("readings").publish().await?;
        bus.message(&Sent(b"3")).to("audit").publish().await?;
        bus.message(&Sent(b"7")).to("local").publish().await?;

        bus.subscriber("readings")
            .assert_called(1)
            .settled(HandlerOutcome::ack());
        bus.subscriber("audit").assert_called(1);
        bus.subscriber("local")
            .assert_called(1)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}
