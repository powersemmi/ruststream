//! The compio `TestApp` runs a local memory broker of any capacity, and refuses a broker type
//! that has no in-process mode.

#![allow(clippy::future_not_send)]

use std::error::Error;

use compio::runtime::Runtime;
use ruststream::Broker;
use ruststream::memory::local::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError, LocalMemorySource,
};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::TestError;
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// A reading, as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// A reading the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "readings")]
struct Sent(&'static [u8]);

/// Acknowledges every reading.
#[subscriber(LocalMemorySource::new("readings"))]
async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

#[test]
fn a_capacity_of_seven_is_started_and_handles_a_publish() -> Result<(), Box<dyn Error>> {
    type Bus = LocalMemoryBroker<7>;
    Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("readings", "0.1.0"))
            .with_broker(Bus::new(), |b| b.include(on_reading));
        let tb = TestApp::start(app).await?;
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

#[test]
fn a_capacity_of_three_is_started_and_handles_a_publish() -> Result<(), Box<dyn Error>> {
    type Bus = LocalMemoryBroker<3>;
    Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("readings", "0.1.0"))
            .with_broker(Bus::new(), |b| b.include(on_reading));
        let tb = TestApp::start(app).await?;
        tb.broker::<Bus>().message(&Sent(b"22")).publish().await?;
        tb.broker::<Bus>()
            .subscriber("readings")
            .assert_called_once()
            .with_raw(b"22");
        tb.shutdown().await?;
        Ok(())
    })
}

/// A broker of its own type, which no crate registered with the harness.
struct Unregistered(LocalMemoryBroker<4>);

impl Broker for Unregistered {
    type Error = LocalMemoryError;
    type Connected = ConnectedLocalMemoryBroker<4>;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        self.0.connect()
    }
}

#[test]
fn a_broker_type_nobody_registered_is_refused() -> Result<(), Box<dyn Error>> {
    let started = Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("readings", "0.1.0"))
            .with_broker(Unregistered(LocalMemoryBroker::new()), |b| b);
        TestApp::start(app).await.map(|_| ())
    });
    assert!(
        matches!(started, Err(TestError::NoTransport(_))),
        "the start returned {started:?}",
    );
    Ok(())
}
