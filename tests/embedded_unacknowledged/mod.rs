//! A transport with no acknowledgement to give, on the embedded runner: the scenario both
//! diagnostics suites run, each with the logger it selects.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::error::Error;

use futures::{Stream, StreamExt};
use ruststream::diagnostics::Logger;
#[cfg(not(ruststream_alloc))]
use ruststream::memory::local::LocalMemoryBus;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, IntoSource, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{
    AckError, Broker, BrokerMoves, Deserialized, Headers, IncomingMessage, Outgoing, Serialized,
    Settle, Subscriber, SubscriptionSource, subscriber,
};

/// A reading, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// The bus of the unsupported scenario, and its connected form.
type Wide = LocalMemoryBroker<2>;
type Wired = <Wide as Broker>::Connected;

/// The subscription of a transport that has no acknowledgement to give: it reads the local bus and
/// answers every settlement [`AckError::Unsupported`], as MQTT `QoS 0` or a socket does.
#[derive(Clone, Copy)]
struct Unacknowledged(&'static str);

/// The deliveries of [`Unacknowledged`].
struct Unacknowledging(<LocalMemorySource as SubscriptionSource<Wired>>::Subscriber);

/// One delivery of the bus whose settlement the transport cannot perform.
struct Unsettled(
    <<LocalMemorySource as SubscriptionSource<Wired>>::Subscriber as Subscriber>::Message,
);

impl Unacknowledged {
    const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl IntoSource for Unacknowledged {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<Wired> for Unacknowledged {
    type Subscriber = Unacknowledging;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.0
    }

    fn subscribe(
        self,
        connected: &Wired,
    ) -> impl Future<Output = Result<Unacknowledging, <Wide as Broker>::Error>> {
        let opened = LocalMemorySource::new(self.0).subscribe(connected);
        async move { Ok(Unacknowledging(opened.await?)) }
    }
}

impl Subscriber for Unacknowledging {
    type Message = Unsettled;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Unsettled, Infallible>> + '_ {
        self.0.stream().map(|delivery| match delivery {
            Ok(delivery) => Ok(Unsettled(delivery)),
            Err(never) => match never {},
        })
    }
}

impl IncomingMessage for Unsettled {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &Headers {
        self.0.headers()
    }
}

impl Settle for Unsettled {
    async fn ack(self) -> Result<(), AckError> {
        // The bus would requeue what is dropped unsettled, and a transport that settles nothing
        // does not redeliver.
        self.0.nack(false).await?;
        Err(AckError::Unsupported)
    }

    async fn nack(self, _requeue: bool) -> Result<(), AckError> {
        self.0.nack(false).await?;
        Err(AckError::Unsupported)
    }
}

/// Acknowledges a reading the transport cannot acknowledge.
#[subscriber(Unacknowledged::new("readings"))]
async fn on_reading(reading: &Job<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

/// A reading for the board.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "readings")]
struct Reading(&'static [u8]);

/// The bus of the unsupported scenario.
#[cfg(not(ruststream_alloc))]
static UNSUPPORTED_BUS: LocalMemoryBus<2> = LocalMemoryBus::new();

/// The broker of the scenario: on a bus of its own where the heap variant has none to name.
fn wide() -> Wide {
    #[cfg(ruststream_alloc)]
    {
        Wide::new()
    }
    #[cfg(not(ruststream_alloc))]
    {
        Wide::new(&UNSUPPORTED_BUS)
    }
}

/// The same app with no logger selected: the heap variant's binary runs it, the variant without
/// `alloc` selects a logger in every test of its own.
#[cfg(ruststream_alloc)]
pub(crate) async fn run_unselected() -> Result<(), Box<dyn Error>> {
    let app = EmbeddedApp::new().with_broker(wide(), |b| b.include(on_reading));
    drive(app).await
}

/// Starts the app whose transport answers every settlement unsupported, with `logger` selected,
/// and lets one reading through.
///
pub(crate) async fn run<L: Logger>(logger: L) -> Result<(), Box<dyn Error>> {
    let app = EmbeddedApp::new()
        .logger(logger)
        .with_broker(wide(), |b| b.include(on_reading));
    drive(app).await
}

/// Lets one reading through `app`.
async fn drive(app: EmbeddedApp<impl Mounts + 'static>) -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app).await?;
    tb.broker::<Wide>()
        .message(&Reading(b"21"))
        .publish()
        .await?;
    tb.settle().await?;
    tb.shutdown().await?;
    Ok(())
}
