//! A compio service's handler runs and acknowledges, through the compio `TestApp`.

// The app and its handlers are not driven by a multi-thread runtime: the harness runs them on the
// test's own compio runtime.
#![allow(clippy::future_not_send)]

use std::error::Error;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// A reading, as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// A reading the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "readings")]
struct Sent(&'static [u8]);

// --8<-- [start:service]
/// Acknowledges every reading.
#[subscriber(LocalMemorySource::new("readings"))]
async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("readings", "0.1.0"))
        .with_broker(Bus::new(), |b| b.include(on_reading))
}
// --8<-- [end:service]

// --8<-- [start:test]
#[test]
fn a_published_reading_is_handled_and_acknowledged() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;

        tb.broker::<Bus>()
            .subscriber("readings")
            .assert_called_once()
            .with_raw(b"21")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:test]

#[test]
fn readings_one_after_the_other_are_all_handled() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        for reading in [b"21", b"22", b"23"] {
            tb.broker::<Bus>().message(&Sent(reading)).publish().await?;
        }

        tb.broker::<Bus>()
            .subscriber("readings")
            .assert_called(3)
            .with_raw(b"23");
        tb.shutdown().await?;
        Ok(())
    })
}
