//! Assertion builders over what the harness recorded: per-subscriber deliveries
//! ([`SubscriberAssertions`]) and per-channel publishes ([`PublishedAssertions`]).
//!
//! Every method asserts (panicking with a descriptive message on failure) and returns `self`, so
//! checks chain: `subscriber("orders").assert_called_once().with(&Order { id: 1 }).settled(Ack)`.

// These methods run their check eagerly (panicking on failure) and return `self` only for optional
// chaining, so they are deliberately not `#[must_use]` (ending a chain on one is fine).
#![allow(clippy::return_self_not_must_use, clippy::must_use_candidate)]

use std::marker::PhantomData;

use bytes::Bytes;

use crate::RawMessage;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::{Codec, DefaultCodec};
use crate::runtime::HandlerResult;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use serde::de::DeserializeOwned;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use std::fmt::Debug;

use super::coordinator::{Coordinator, Outcome, Record};

/// Assertions over the deliveries one subscriber received, recorded by the harness.
#[derive(Debug)]
pub struct SubscriberAssertions<'a> {
    coordinator: &'a Coordinator,
    scope_id: usize,
    name: String,
}

impl<'a> SubscriberAssertions<'a> {
    pub(crate) fn new(coordinator: &'a Coordinator, scope_id: usize, name: String) -> Self {
        Self {
            coordinator,
            scope_id,
            name,
        }
    }

    /// Runs `f` over the recorded deliveries to this subscriber, in delivery order.
    fn with_records<R>(&self, f: impl FnOnce(&[&Record]) -> R) -> R {
        self.coordinator.with_records(self.scope_id, &self.name, f)
    }

    /// Runs `f` over the most recent recorded delivery, panicking if there were none.
    fn with_last<R>(&self, what: &str, f: impl FnOnce(&Record) -> R) -> R {
        self.with_records(|records| {
            let last = records.last().unwrap_or_else(|| {
                panic!(
                    "subscriber {:?} was not called, cannot assert {what}",
                    self.name
                )
            });
            f(last)
        })
    }

    /// Asserts this subscriber received exactly one delivery.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was called zero or more than one time.
    pub fn assert_called_once(self) -> Self {
        let count = self.with_records(|records| records.len());
        assert_eq!(
            count, 1,
            "subscriber {:?} was called {count} times, expected exactly once",
            self.name,
        );
        self
    }

    /// Asserts this subscriber received exactly `times` deliveries (counting each redelivery).
    ///
    /// # Panics
    ///
    /// Panics if the delivery count differs from `times`.
    pub fn assert_called(self, times: usize) -> Self {
        let count = self.with_records(|records| records.len());
        assert_eq!(
            count, times,
            "subscriber {:?} was called {count} times, expected {times}",
            self.name,
        );
        self
    }

    /// Every raw payload this subscriber received, in delivery order, for custom inspection beyond
    /// the built-in assertions.
    #[must_use]
    pub fn received_raw(&self) -> Vec<Bytes> {
        self.with_records(|records| records.iter().map(|record| record.raw.clone()).collect())
    }

    /// Decodes every payload this subscriber received (with
    /// [`DefaultCodec`](crate::codec::DefaultCodec)), in delivery order, for custom inspection. Use
    /// [`received_with`](Self::received_with) for a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if any received payload fails to decode as `T` (a delivery the handler rejected as a
    /// decode failure will fail here too).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    #[must_use]
    pub fn received<T: DeserializeOwned>(&self) -> Vec<T> {
        self.received_with(&DefaultCodec::default())
    }

    /// Like [`received`](Self::received), but decodes with `codec` - use it when the handler was
    /// mounted with a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if any received payload fails to decode as `T`.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    #[must_use]
    pub fn received_with<T, C>(&self, codec: &C) -> Vec<T>
    where
        T: DeserializeOwned,
        C: Codec,
    {
        self.with_records(|records| {
            records
                .iter()
                .map(|record| {
                    codec.decode(&record.raw).unwrap_or_else(|err| {
                        panic!(
                            "subscriber {:?} received a payload that did not decode as {}: {err}",
                            self.name,
                            std::any::type_name::<T>(),
                        )
                    })
                })
                .collect()
        })
    }

    /// Asserts this subscriber was never called.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber received any delivery.
    pub fn assert_not_called(self) {
        let count = self.with_records(|records| records.len());
        assert_eq!(
            count, 0,
            "subscriber {:?} was called {count} times, expected never",
            self.name,
        );
    }

    /// Asserts the most recent delivery's payload decodes (with
    /// [`DefaultCodec`](crate::codec::DefaultCodec)) to `expected`. If the handler was mounted with a
    /// different codec, use [`with_codec`](Self::with_codec).
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, the payload fails to decode, or the decoded value
    /// differs from `expected`.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn with<T>(self, expected: &T) -> Self
    where
        T: DeserializeOwned + PartialEq + Debug,
    {
        self.with_codec(&DefaultCodec::default(), expected)
    }

    /// Like [`with`](Self::with), but decodes the recorded payload with `codec` - use it when the
    /// handler was mounted with a non-default codec (`include_with` / `with_broker_codec`).
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, the payload fails to decode, or the decoded value
    /// differs from `expected`.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn with_codec<T, C>(self, codec: &C, expected: &T) -> Self
    where
        T: DeserializeOwned + PartialEq + Debug,
        C: Codec,
    {
        self.with_last("the received value", |record| {
            let actual: T = codec.decode(&record.raw).unwrap_or_else(|err| {
                panic!(
                    "subscriber {:?} received a payload that did not decode as {}: {err}",
                    self.name,
                    std::any::type_name::<T>(),
                )
            });
            assert_eq!(
                &actual, expected,
                "subscriber {:?} received an unexpected value",
                self.name
            );
        });
        self
    }

    /// Asserts the most recent delivery's raw payload equals `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or the raw payload differs.
    pub fn with_raw(self, bytes: &[u8]) -> Self {
        self.with_last("the raw payload", |record| {
            assert_eq!(
                record.raw.as_ref(),
                bytes,
                "subscriber {:?} received unexpected raw bytes",
                self.name,
            );
        });
        self
    }

    /// Asserts the most recent delivery settled with `outcome`.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or settled differently (a fail-fast panic leaves the
    /// message unsettled, which never matches).
    pub fn settled(self, outcome: HandlerResult) -> Self {
        self.with_last("the settlement", |record| {
            assert_eq!(
                record.settle,
                Some(outcome),
                "subscriber {:?} settled differently than expected",
                self.name,
            );
        });
        self
    }

    /// Asserts the handler panicked on the most recent delivery.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or the handler did not panic.
    pub fn panicked(self) -> Self {
        self.with_last("a panic", |record| {
            assert!(
                record.panicked,
                "subscriber {:?} did not panic on its last delivery",
                self.name,
            );
        });
        self
    }

    /// Asserts the most recent delivery's classified [`Outcome`] equals `expected` (a single check
    /// covering ack / nack / drop / decode-failure / panic).
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or its last outcome differs.
    pub fn assert_outcome(self, expected: Outcome) -> Self {
        self.with_last("the outcome", |record| {
            assert_eq!(
                record.outcome(),
                expected,
                "subscriber {:?} had an unexpected outcome",
                self.name,
            );
        });
        self
    }

    /// Asserts the most recent delivery's payload failed to decode into the handler's input type.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or the payload decoded successfully.
    pub fn assert_last_failed_to_decode(self) {
        self.with_last("a decode failure", |record| {
            assert!(
                record.decode_failed,
                "subscriber {:?} decoded its last delivery successfully",
                self.name,
            );
        });
    }
}

/// Assertions over what was published to one channel, read from the broker's publish log. The type
/// parameter `T` is the expected payload type for [`with`](Self::with).
#[derive(Debug)]
pub struct PublishedAssertions<T> {
    name: String,
    messages: Vec<RawMessage>,
    _payload: PhantomData<fn() -> T>,
}

impl<T> PublishedAssertions<T> {
    pub(crate) fn new(name: String, messages: Vec<RawMessage>) -> Self {
        Self {
            name,
            messages,
            _payload: PhantomData,
        }
    }

    /// Asserts exactly one message was published to this channel.
    ///
    /// # Panics
    ///
    /// Panics if zero or more than one message was published.
    pub fn assert_called_once(self) -> Self {
        let count = self.messages.len();
        assert_eq!(
            count, 1,
            "channel {:?} was published to {count} times, expected exactly once",
            self.name,
        );
        self
    }

    /// Asserts nothing was published to this channel.
    ///
    /// # Panics
    ///
    /// Panics if any message was published.
    pub fn assert_not_called(self) {
        let count = self.messages.len();
        assert_eq!(
            count, 0,
            "channel {:?} was published to {count} times, expected never",
            self.name,
        );
    }

    /// Every message published to this channel, in publish order, for custom inspection beyond the
    /// built-in assertions. Bind the assertions to a variable to borrow them:
    /// `let pubs = tb.broker::<B>().published::<T>("x"); for m in pubs.messages() { .. }`.
    #[must_use]
    pub fn messages(&self) -> &[RawMessage] {
        &self.messages
    }

    /// Re-types the expected payload, for assertion sources that do not name one up front
    /// (`tb.out::<Slot>().decoded_as::<Reply>().with(&expected)`).
    #[must_use]
    pub fn decoded_as<U>(self) -> PublishedAssertions<U> {
        PublishedAssertions::new(self.name, self.messages)
    }

    /// The most recent published message, panicking if there were none.
    fn last(&self, what: &str) -> &RawMessage {
        self.messages.last().unwrap_or_else(|| {
            panic!(
                "nothing was published to {:?}, cannot assert {what}",
                self.name
            )
        })
    }

    /// Asserts the most recent published payload equals `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, or the raw payload differs.
    pub fn with_raw(self, bytes: &[u8]) -> Self {
        assert_eq!(
            self.last("the raw payload").payload(),
            bytes,
            "channel {:?} published unexpected raw bytes",
            self.name,
        );
        self
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<T> PublishedAssertions<T>
where
    T: DeserializeOwned + PartialEq + Debug,
{
    /// Asserts the most recent published payload decodes (with
    /// [`DefaultCodec`](crate::codec::DefaultCodec)) to `expected`. If the publisher uses a different
    /// codec, use [`with_codec`](Self::with_codec).
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, the payload fails to decode, or it differs from `expected`.
    pub fn with(self, expected: &T) -> Self {
        self.with_codec(&DefaultCodec::default(), expected)
    }

    /// Like [`with`](Self::with), but decodes the published payload with `codec` - use it when the
    /// publisher was built with a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, the payload fails to decode, or it differs from `expected`.
    pub fn with_codec<C: Codec>(self, codec: &C, expected: &T) -> Self {
        let actual: T = codec
            .decode(self.last("the published value").payload())
            .unwrap_or_else(|err| {
                panic!(
                    "channel {:?} published a payload that did not decode as {}: {err}",
                    self.name,
                    std::any::type_name::<T>(),
                )
            });
        assert_eq!(
            &actual, expected,
            "channel {:?} published an unexpected value",
            self.name
        );
        self
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<T: DeserializeOwned> PublishedAssertions<T> {
    /// Decodes every message published to this channel (with
    /// [`DefaultCodec`](crate::codec::DefaultCodec)), in publish order, for custom inspection. Use
    /// [`decoded_with`](Self::decoded_with) for a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if any published payload fails to decode as `T`.
    #[must_use]
    pub fn decoded(&self) -> Vec<T> {
        self.decoded_with(&DefaultCodec::default())
    }

    /// Like [`decoded`](Self::decoded), but decodes with `codec` - use it when the publisher was
    /// built with a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if any published payload fails to decode as `T`.
    #[must_use]
    pub fn decoded_with<C: Codec>(&self, codec: &C) -> Vec<T> {
        self.messages
            .iter()
            .map(|message| {
                codec.decode(message.payload()).unwrap_or_else(|err| {
                    panic!(
                        "channel {:?} published a payload that did not decode as {}: {err}",
                        self.name,
                        std::any::type_name::<T>(),
                    )
                })
            })
            .collect()
    }
}
