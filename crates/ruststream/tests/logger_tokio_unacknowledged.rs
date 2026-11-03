//! A tokio transport with no acknowledgement to give: the "ack / nack failed" warning reaches the
//! logger the app selects, with its subscription, and the backend's level silences it.
//!
//! The selection is process-wide, so the phases run in order inside one test of this binary.
// The tokio harness's futures are not `Send`; the test drives them on its own thread.
#![allow(clippy::future_not_send)]
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "testing",
    feature = "tokio"
))]

use std::convert::Infallible;
use std::sync::atomic::Ordering;

use futures::{Stream, StreamExt};
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::testing::TestApp;
use ruststream::{AckError, BrokerMoves, Settle, Subscribe, Subscriber, SubscriptionSource};
use tracing::Level;
use tracing::subscriber::set_default;

mod logger_capture;
use logger_capture::{Traced, rank};

/// A reading, as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// A subscription of a transport that has no acknowledgement: every settlement is unsupported,
/// as for a socket or MQTT `QoS 0`.
#[derive(Clone, Copy)]
struct Unacknowledged(&'static str);

impl Unacknowledged {
    const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for Unacknowledged {
    type Subscriber = Unacknowledging;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.0
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<Unacknowledging, MemoryError> {
        Ok(Unacknowledging(connected.subscribe(self.0).await?))
    }
}

/// The deliveries of [`Unacknowledged`].
struct Unacknowledging(MemorySubscriber);

impl Subscriber for Unacknowledging {
    type Message = Unsettled;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Unsettled, Infallible>> + Send + '_ {
        self.0.stream().map(|item| item.map(Unsettled))
    }
}

/// A delivery whose settlement the transport cannot perform.
struct Unsettled(MemoryMessage);

impl IncomingMessage for Unsettled {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }
}

impl Settle for Unsettled {
    async fn ack(self) -> Result<(), AckError> {
        // The in-process queue still learns the delivery is done; the transport says it cannot.
        self.0.ack().await?;
        Err(AckError::Unsupported)
    }

    async fn nack(self, _requeue: bool) -> Result<(), AckError> {
        self.0.ack().await?;
        Err(AckError::Unsupported)
    }
}

/// A reading sent to the app, as its bytes.
#[derive(Outgoing, Serialized)]
struct Feed(Vec<u8>);

#[subscriber(Unacknowledged::new("readings"))]
async fn read(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

/// Delivers one reading to the app, with `logger` selecting the logger when it is set.
async fn deliver(logger: bool) {
    let setup = RustStream::new(AppInfo::new("readings", "0.1.0"));
    let setup = if logger {
        setup.logger(TracingLogger)
    } else {
        setup
    };
    let app = setup.with_broker(MemoryBroker::new(), |b| b.include(read));
    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Feed(b"21.5".to_vec()))
        .to("readings")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.shutdown().await.expect("shutdown");
}

/// Asserts the one warning is the unsupported acknowledgement, with its subscription.
fn assert_unsupported_acknowledgement(lines: &[String]) {
    let lines: Vec<String> = lines
        .iter()
        .filter(|line| line.starts_with("WARN"))
        .cloned()
        .collect();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("WARN ruststream::dispatch:"),
        "{lines:?}"
    );
    assert!(lines[0].contains("ack / nack failed"), "{lines:?}");
    assert!(lines[0].contains("subscription=readings"), "{lines:?}");
    assert!(
        lines[0].contains("error=acknowledgement is not supported by this broker"),
        "{lines:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_unsupported_acknowledgement_is_reported_through_the_selected_logger() {
    // No logger selected: nothing is reported.
    let traced = Traced::at(Level::DEBUG);
    {
        let _scope = set_default(traced.clone());
        deliver(false).await;
    }
    assert!(traced.lines().is_empty(), "{:?}", traced.lines());

    // `TracingLogger`: the subscriber hears the warning.
    {
        let _scope = set_default(traced.clone());
        deliver(true).await;
    }
    assert_unsupported_acknowledgement(&traced.lines());

    // The subscriber's level silences it, and the same event shows at the next level.
    let silenced = Traced::at(Level::ERROR);
    {
        let _scope = set_default(silenced.clone());
        deliver(true).await;
    }
    assert!(silenced.lines().is_empty(), "{:?}", silenced.lines());
    silenced.level.store(rank(Level::WARN), Ordering::Relaxed);
    {
        let _scope = set_default(silenced.clone());
        deliver(true).await;
    }
    assert_unsupported_acknowledgement(&silenced.lines());
}
