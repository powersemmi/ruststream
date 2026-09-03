//! Assertion builders over what the harness recorded: per-subscriber deliveries
//! ([`SubscriberAssertions`]) and per-channel publishes ([`PublishedAssertions`]).
//!
//! Every method asserts (panicking with a descriptive message on failure) and returns `self`, so
//! checks chain: `subscriber("orders").assert_called_once().with(&Order { id: 1 }).settled(Ack)`.

// These methods run their check eagerly (panicking on failure) and return `self` only for optional
// chaining, so ending a chain on one is fine and none of them is `#[must_use]`.
#![allow(clippy::return_self_not_must_use, clippy::must_use_candidate)]

use std::marker::PhantomData;

use bytes::Bytes;

use crate::RawMessage;
use crate::codec::Codec;
// Only the DEFAULT codec is feature-dependent; the assertions that take a codec are always here.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::runtime::HandlerOutcome;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

use super::coordinator::{Coordinator, Delivered, Outcome, Record};

/// Assertions over the deliveries one subscriber received, recorded by the harness.
///
/// The unit these count is the handler CALL, not the message: a single-message handler is called
/// once per delivery, and a batch handler once per page. So `assert_called_once` on a batch
/// subscription means one page arrived, whatever its size, while
/// [`received_raw`](Self::received_raw) still lists every element of it. An element the decode
/// policy rejected before the body ran is settled by that policy and is not part of the page the
/// handler was called with, so it does not appear here.
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

    /// Runs `f` over the most recent recorded call, panicking if there were none.
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

    /// Runs `f` over the sole delivery of the most recent call. The assertions that name one
    /// expected payload go through here, so a page of several reports what it was instead of
    /// silently checking one element of it.
    fn with_sole_delivery<R>(&self, what: &str, f: impl FnOnce(&Delivered) -> R) -> R {
        self.with_last(what, |record| match record.deliveries.as_slice() {
            [only] => f(only),
            page => panic!(
                "subscriber {:?} last received a page of {} deliveries, so it has no single \
                 {what}; read the page with received_raw()",
                self.name,
                page.len(),
            ),
        })
    }

    /// Asserts this subscriber was called exactly once (one delivery, or one page).
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
    /// the built-in assertions. A page contributes its elements, so the list is flat whether the
    /// handler takes one message or a slice.
    #[must_use]
    pub fn received_raw(&self) -> Vec<Bytes> {
        self.with_records(|records| {
            records
                .iter()
                .flat_map(|record| record.deliveries.iter().map(|one| one.raw.clone()))
                .collect()
        })
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
    #[must_use]
    pub fn received_with<T, C>(&self, codec: &C) -> Vec<T>
    where
        T: DeserializeOwned,
        C: Codec,
    {
        self.with_records(|records| {
            records
                .iter()
                .flat_map(|record| record.deliveries.iter())
                .map(|one| {
                    codec.decode(&one.raw).unwrap_or_else(|err| {
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
    pub fn with_codec<T, C>(self, codec: &C, expected: &T) -> Self
    where
        T: DeserializeOwned + PartialEq + Debug,
        C: Codec,
    {
        self.with_sole_delivery("received value", |one| {
            let actual: T = codec.decode(&one.raw).unwrap_or_else(|err| {
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

    /// Asserts the most recent call carried one delivery whose raw payload equals `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, the raw payload differs, or the most recent call
    /// was a page of several deliveries (which has no single payload - read it with
    /// [`received_raw`](Self::received_raw)).
    pub fn with_raw(self, bytes: &[u8]) -> Self {
        self.with_sole_delivery("raw payload", |one| {
            assert_eq!(
                one.raw.as_ref(),
                bytes,
                "subscriber {:?} received unexpected raw bytes",
                self.name,
            );
        });
        self
    }

    /// Asserts the most recent call settled with `outcome`'s status (any continuation on the
    /// expected value is ignored: the harness compares how the broker settled).
    ///
    /// A page settles per element, so this asserts EVERY element of the most recent page settled
    /// that way - which is what a uniform answer (`HandlerOutcome::ack()` for the whole slice)
    /// produces. A page that answered element by element with differing outcomes matches none.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or anything it last received settled differently
    /// (a fail-fast panic leaves the message unsettled, which never matches).
    // The expectation is a just-built outcome token (`settled(HandlerOutcome::ack())`); a
    // reference parameter would force `&` noise at every call site.
    #[allow(clippy::needless_pass_by_value)]
    pub fn settled(self, outcome: HandlerOutcome) -> Self {
        let expected = Some(outcome.outcome());
        self.with_last("the settlement", |record| {
            let mismatched = record
                .deliveries
                .iter()
                .filter(|one| one.settle != expected)
                .count();
            assert_eq!(
                mismatched,
                0,
                "subscriber {:?} settled {mismatched} of its last {} deliveries differently \
                 than expected",
                self.name,
                record.deliveries.len(),
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
}

impl<T> PublishedAssertions<T>
where
    T: DeserializeOwned + PartialEq + Debug,
{
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
}

impl<T: DeserializeOwned> PublishedAssertions<T> {
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

#[cfg(all(test, feature = "json"))]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Order {
        id: u64,
    }

    fn undecodable() -> PublishedAssertions<Order> {
        PublishedAssertions::new(
            "orders".to_owned(),
            vec![RawMessage::new("orders", b"not json".as_slice())],
        )
    }

    #[test]
    fn published_assertions_read_back_what_was_logged() {
        let logged = PublishedAssertions::<Order>::new(
            "orders".to_owned(),
            vec![RawMessage::new("orders", br#"{"id":7}"#.as_slice())],
        );
        assert_eq!(logged.decoded_with(&JsonCodec), vec![Order { id: 7 }]);
        logged.with_codec(&JsonCodec, &Order { id: 7 });
    }

    #[test]
    fn an_empty_channel_names_itself_when_asserted_on() {
        let empty = PublishedAssertions::<Order>::new("orders".to_owned(), Vec::new());
        let failure = std::panic::catch_unwind(move || empty.decoded_with(&JsonCodec));
        assert!(
            failure.is_ok(),
            "no messages is an empty result, not a panic"
        );
    }

    // A decode failure inside an assertion is a test-authoring mistake, so the panic has to name
    // the channel and the type it could not produce.
    #[test]
    #[should_panic(expected = "channel \"orders\" published a payload that did not decode")]
    fn a_published_payload_that_does_not_decode_names_the_channel_and_type() {
        undecodable().with_codec(&JsonCodec, &Order { id: 7 });
    }

    #[test]
    #[should_panic(expected = "did not decode as")]
    fn decoding_every_published_payload_reports_the_first_failure() {
        let _ = undecodable().decoded_with(&JsonCodec);
    }

    #[test]
    #[should_panic(expected = "nothing was published to \"orders\"")]
    fn asserting_on_a_channel_that_published_nothing_says_so() {
        let empty = PublishedAssertions::<Order>::new("orders".to_owned(), Vec::new());
        empty.with_codec(&JsonCodec, &Order { id: 7 });
    }
}
