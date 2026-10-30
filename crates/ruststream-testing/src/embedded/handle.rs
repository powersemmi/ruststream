//! [`BrokerHandle`], the per-broker handle of an embedded [`TestApp`]: where a test's input
//! enters a broker, and where the assertions on that broker's handlers and publishes start.

use std::fmt;

use ruststream_core::{Lend, OutgoingDestination, OutgoingMessage, RawMessage};
use ruststream_runtime::testing::{
    PublishTarget, PublishedAssertions, RunBroker, SubscriberAssertions, TestError, attribute,
};
use ruststream_runtime::{
    HeadersUnset, LocalPublishSink, MessageBody, PublishBuilder, UnnamedCodec, message_of,
};

use super::TestApp;

/// One broker of an embedded [`TestApp`], addressed by its production type with
/// [`TestApp::broker`].
///
/// A test publishes its input through [`message`](Self::message), and reads what the broker's
/// subscriptions handled through [`subscriber`](Self::subscriber) and what reached a destination
/// through [`published`](Self::published).
///
/// # Examples
///
/// See [`TestApp`].
pub struct BrokerHandle<'a> {
    app: &'a TestApp,
    broker: RunBroker,
}

impl fmt::Debug for BrokerHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerHandle")
            .field("broker", &self.broker.name())
            .finish_non_exhaustive()
    }
}

/// Where a test's publish enters the broker: the in-process transport, as an external producer
/// would publish. After the broker takes the message, the publish lets the app handle it.
///
/// # Examples
///
/// See [`TestApp`].
pub struct InjectSink<'a> {
    app: &'a TestApp,
    broker: RunBroker,
}

impl fmt::Debug for InjectSink<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InjectSink").finish_non_exhaustive()
    }
}

impl LocalPublishSink for InjectSink<'_> {
    // The in-process transport copies what it keeps out of the bytes it is lent.
    type Payload = Lend;
    type Error = TestError;
    // An injection stands in for an external producer, which reaches no broker's options type.
    type Options = ();

    async fn send(
        &mut self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let testable = self.broker.testable().ok_or(TestError::ShutDown)?;
        let coordinator = self.app.coordinator();
        // The test's own publish is part of the record: it tells a message the test put on a
        // destination from one the app published there.
        let recorded = RawMessage::new(msg.name().to_owned(), msg.payload().to_vec())
            .with_headers(msg.headers().clone());
        let started = coordinator.test_publish_started();
        testable.inject(msg);
        coordinator
            .record_published(self.broker.scope_id(), recorded, started)
            .run()
            .await;
        self.app.settle().await
    }
}

impl<'a> BrokerHandle<'a> {
    pub(super) const fn new(app: &'a TestApp, broker: RunBroker) -> Self {
        Self { app, broker }
    }

    /// Starts a publish of `value` onto this broker, as an external producer would publish it;
    /// the publish lets the app handle it before it returns.
    ///
    /// The destination is the type's `#[outgoing(name = ..)]`, or the builder's `.to(name)`.
    ///
    /// # Examples
    ///
    /// See [`TestApp`].
    pub fn message<'v, T>(
        &self,
        value: &'v T,
    ) -> PublishBuilder<InjectSink<'a>, MessageBody<'v, T>, UnnamedCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        let sink = InjectSink {
            app: self.app,
            broker: self.broker.clone(),
        };
        message_of(sink, value, UnnamedCodec::new())
    }

    /// The assertions on what this broker's subscription `name` handled.
    ///
    /// # Examples
    ///
    /// See [`TestApp`].
    #[must_use]
    pub fn subscriber(&self, name: &str) -> SubscriberAssertions<'_> {
        SubscriberAssertions::new(
            self.app.coordinator(),
            self.broker.scope_id(),
            name.to_owned(),
        )
    }

    /// The assertions on what reached the destination `name` on this broker: the test's
    /// publishes and the app's, in the order the broker took them.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
    /// use ruststream::runtime::{EmbeddedApp, Mounts};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
    ///
    /// type Bus = LocalMemoryBroker<4>;
    ///
    /// /// A temperature frame, as the sensor sends its bytes.
    /// #[derive(Deserialized)]
    /// struct Reading<'a>(&'a [u8]);
    ///
    /// /// What the display shows.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "display/temp")]
    /// struct Shown(Vec<u8>);
    ///
    /// /// Shows every reading on the display, in degrees.
    /// #[subscriber(LocalMemorySource::new("sensors/temp"), reply)]
    /// async fn show(reading: &Reading<'_>) -> Shown {
    ///     Shown([reading.0, b" C"].concat())
    /// }
    ///
    /// fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new().with_broker(bus, |b| b.include(show))
    /// }
    ///
    /// /// A frame as the test's sensor sends it.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "sensors/temp")]
    /// struct Sent(&'static [u8]);
    ///
    /// async fn shows_the_reading() -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::start(app(Bus::new())).await?;
    ///     tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;
    ///
    ///     tb.broker::<Bus>()
    ///         .published::<()>("display/temp")
    ///         .assert_called_once()
    ///         .with_raw(b"21 C");
    ///     tb.shutdown().await?;
    ///     Ok(())
    /// }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     embassy_futures::block_on(shows_the_reading())
    /// # }
    /// ```
    #[must_use]
    pub fn published<T>(&self, name: &str) -> PublishedAssertions<T> {
        let coordinator = self.app.coordinator();
        let scope_id = self.broker.scope_id();
        let recorded = coordinator.published(scope_id, name);
        let (messages, options) = self.broker.testable().map_or_else(
            || (Vec::new(), Vec::new()),
            |testable| {
                let messages = testable.published(name);
                let options = attribute(&messages, &recorded, testable.evicted(name));
                (messages, options)
            },
        );
        let target = PublishTarget::Channel {
            scope_id,
            name: name.to_owned(),
        };
        PublishedAssertions::new(coordinator, target, name.to_owned(), messages, options)
    }
}
