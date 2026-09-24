//! [`BrokerHandle`], the per-broker handle of a [`TestApp`](super::TestApp): where a test's input enters a broker,
//! and where the assertions on that broker's handlers and publishes start.

use std::any::Any;
use std::fmt;

// The helpers that ENCODE stay gated on a codec feature, like the codec itself; the typed
// builder entry points do not, because the value's own wire decides whether a codec is needed.
use crate::OutgoingDestination;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::{Codec, DefaultCodec};
use crate::runtime::{HeadersUnset, PublishBuilder, PublishSink, Shutdown};
use crate::runtime::{MessageBody, UnnamedCodec, message_of};
use crate::{Lend, OutgoingMessage, RawMessage};

use super::assertions::{PublishedAssertions, SubscriberAssertions};
use super::broker::{LivePublish, TestableBroker};
use super::coordinator::Coordinator;
use super::harness::{BrokerEntry, Mode, TestError};

/// A handle to one broker in a [`TestApp`](super::TestApp): inject input and assert on its handlers and publishes.
pub struct BrokerHandle<'a> {
    pub(super) harness: Harness<'a>,
    pub(super) scope_id: usize,
    pub(super) transport: Transport<'a>,
    pub(super) label: String,
}

/// What a handle and its injection sink borrow from the harness.
#[derive(Clone, Copy)]
pub(super) struct Harness<'a> {
    pub(super) coordinator: &'a Coordinator,
    pub(super) shutdown: &'a Shutdown,
    pub(super) mode: &'a Mode,
    pub(super) brokers: &'a [BrokerEntry],
}

/// Where a handle's injections go.
#[derive(Clone, Copy)]
pub(super) enum Transport<'a> {
    /// The broker's in-process transport, which takes an injection synchronously.
    InProcess(&'a dyn TestableBroker),
    /// The live connected broker, and how a test's input is published onto it: `None` for an
    /// untyped handle onto a broker type no crate registered.
    Live {
        connected: &'a (dyn Any + Send + Sync),
        publish: Option<LivePublish>,
    },
}

impl fmt::Debug for BrokerHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerHandle")
            .field("broker", &self.label)
            .finish_non_exhaustive()
    }
}

/// The publish builder's sink for the harness.
///
/// It injects the message onto a broker's in-process transport the way an external producer
/// would, then drives the resulting reaction to a standstill before the publish returns.
///
/// Produced by the `message(..)` entry point of [`TestApp`](super::TestApp) and [`BrokerHandle`], so a test
/// injects through the same positions - destination, typed headers, codec - that the service
/// itself publishes through. You never name this type.
pub struct InjectSink<'a>(pub(super) Target<'a>);

/// What an [`InjectSink`] sends into: a resolved broker, or none because the unscoped entry
/// point had more than one to choose from.
///
/// The unscoped `message(..)` exists whatever the app registered, so the ambiguity rides here
/// and surfaces from the publish.
pub(super) enum Target<'a> {
    Broker {
        harness: Harness<'a>,
        scope_id: usize,
        transport: Transport<'a>,
        label: String,
    },
    Ambiguous,
}

impl fmt::Debug for InjectSink<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InjectSink").finish_non_exhaustive()
    }
}

impl PublishSink for InjectSink<'_> {
    // An injection is handed to the in-process transport by reference, which stores what it
    // keeps: see `TestableBroker::inject`.
    type Payload = Lend;
    type Error = TestError;
    // An injection stands in for an external producer, which reaches no broker's options type.
    type Options = ();

    async fn send(
        &mut self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let Target::Broker {
            harness,
            scope_id,
            transport,
            label,
        } = &self.0
        else {
            return Err(TestError::Ambiguous);
        };
        if harness.shutdown.is_cancelled() {
            return Err(TestError::ShutDown);
        }
        match transport {
            Transport::InProcess(testable) => testable.inject(msg),
            Transport::Live { connected, publish } => {
                let publish = publish.ok_or_else(|| TestError::NoTransport(label.clone()))?;
                // Live, the test's own publish is part of what `published` reads and of what a
                // settle waits for the subscriptions to handle, once the broker took it.
                let recorded = RawMessage::new(msg.name().to_owned(), msg.payload().to_vec())
                    .with_headers(msg.headers().clone());
                publish(*connected, msg)
                    .await
                    .map_err(|err| TestError::Publish {
                        broker: label.clone(),
                        source: err,
                    })?;
                harness.coordinator.record_published(*scope_id, recorded);
            }
        }
        harness
            .mode
            .settle(harness.coordinator, harness.brokers)
            .await
    }
}

impl<'a> BrokerHandle<'a> {
    /// Starts a typed injection of a `#[derive(Outgoing)]` value onto this broker, encoded with
    /// [`DefaultCodec`](crate::codec::DefaultCodec) unless the call names one with
    /// `with_codec(..)`: `handle.message(&order).to("orders").publish().await?`.
    ///
    /// The same builder the service publishes through, sending onto the in-process transport as
    /// an external producer would; awaiting the publish drives the resulting reaction to a
    /// standstill, so the assertions that follow see a settled service.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{Outgoing, subscriber};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Outgoing, Serialize, Deserialize)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u32,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn handle(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| { b.include(handle); });
    /// let tb = TestApp::start(app).await?;
    ///
    /// tb.broker::<MemoryBroker>()
    ///     .message(&Order { id: 7 })
    ///     .publish()
    ///     .await?;
    /// tb.broker::<MemoryBroker>()
    ///     .subscriber("orders")
    ///     .assert_called_once();
    /// # Ok(())
    /// # }
    /// ```
    pub fn message<'v, T>(
        &self,
        value: &'v T,
    ) -> PublishBuilder<InjectSink<'a>, MessageBody<'v, T>, UnnamedCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        // The harness carries no codec of its own, so the position stays unnamed - the same
        // bottom of the ladder a bare publisher uses.
        message_of(self.sink(), value, UnnamedCodec::new())
    }

    /// This handle's transport as a publish sink. It borrows the app, not the handle, so a
    /// builder started on a temporary handle outlives it.
    pub(super) fn sink(&self) -> InjectSink<'a> {
        InjectSink(Target::Broker {
            harness: self.harness,
            scope_id: self.scope_id,
            transport: self.transport,
            label: self.label.clone(),
        })
    }
}

impl BrokerHandle<'_> {
    /// Publishes `value` (encoded with [`DefaultCodec`](crate::codec::DefaultCodec)) to `name`, then
    /// drives the resulting reaction to a standstill before returning.
    ///
    /// Use it for a `Serialize` type the test does not own: [`message`](Self::message) reads the
    /// destination form off the value's type through [`OutgoingDestination`], which the orphan
    /// rule keeps a foreign type from declaring. Inject an owned type through the builder.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::ShutDown`] if the service has been torn down, [`TestError::Encode`]
    /// if the value does not encode, [`TestError::Publish`] if a live broker refuses the message,
    /// or [`TestError::NotQuiescent`] / [`TestError::NotSettled`] if the reaction does not settle.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub async fn publish<T: serde::Serialize + Sync>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), TestError> {
        let bytes = DefaultCodec::default()
            .encode(value)
            .map_err(|err| TestError::Encode(err.to_string()))?;
        self.sink()
            .send(OutgoingMessage::new(name, &bytes), None)
            .await
    }

    /// Like [`publish`](Self::publish), but with headers on the delivery: `headers` is a typed
    /// contract serialized into the header map (see
    /// [`HeaderMap::insert_typed`](crate::HeaderMap::insert_typed)) - the input a
    /// [`Headers`](crate::runtime::Headers) handler parses.
    ///
    /// Same reach as [`publish`](Self::publish): the builder's
    /// `message(&value).with_headers(&meta)` needs the value's type to declare a destination.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Encode`] if the value or the headers do not encode, plus the errors
    /// [`publish`](Self::publish) reports.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub async fn publish_with_headers<T, H>(
        &self,
        name: &str,
        value: &T,
        headers: &H,
    ) -> Result<(), TestError>
    where
        T: serde::Serialize + Sync,
        H: serde::Serialize + Sync,
    {
        let bytes = DefaultCodec::default()
            .encode(value)
            .map_err(|err| TestError::Encode(err.to_string()))?;
        let msg = OutgoingMessage::new(name, &bytes)
            .with_typed_headers(headers)
            .map_err(|err| TestError::Encode(err.to_string()))?;
        self.sink().send(msg, None).await
    }

    /// Asserts on what the handler subscribed to `name` received and how it settled.
    #[must_use]
    pub fn subscriber(&self, name: &str) -> SubscriberAssertions<'_> {
        SubscriberAssertions::new(self.harness.coordinator, self.scope_id, name.to_owned())
    }

    /// Asserts on what was published to `name` on this broker.
    ///
    /// In process it reads the broker's own publish log, which holds everything the transport
    /// took. Live the broker's log is out of reach, so it reads the harness's record: the test's
    /// own publishes onto this broker, and every message a publisher the runtime paired against
    /// this broker handed it, as the framework handed it over. That covers replies, `Out` slot
    /// publishes and requests, retry copies and dead letters, and a `Bound` token's publishes
    /// through any of them. A publisher the service uses itself, an `after_startup` hook's
    /// included, is outside the record.
    ///
    /// The record is the message before the broker's own publisher sees it: a setting that
    /// publisher turns into a header or a destination prefix is not in it, and a transaction is
    /// committed on the broker, out of the harness's sight. A test that runs in both modes
    /// asserts on the recorded form, and asserts on those in process.
    #[must_use]
    pub fn published<T>(&self, name: &str) -> PublishedAssertions<T> {
        let coordinator = self.harness.coordinator;
        let messages = match self.transport {
            Transport::InProcess(testable) => testable.published(name),
            Transport::Live { .. } => coordinator.published(self.scope_id, name),
        };
        PublishedAssertions::new(name.to_owned(), messages, coordinator.reply_published(name))
    }
}

#[cfg(test)]
mod tests {
    use super::{InjectSink, Target};

    /// The sink travels inside a publish builder, which never hands it back, so its `Debug` is
    /// reachable only from here - and it still has to name the type rather than leak the broker
    /// handle it borrows.
    #[test]
    fn the_injection_sink_names_itself_without_its_broker() {
        let sink = InjectSink(Target::Ambiguous);
        assert_eq!(format!("{sink:?}"), "InjectSink { .. }");
    }
}
