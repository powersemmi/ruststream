//! Assertion builders over what the harness recorded: per-subscriber deliveries
//! ([`SubscriberAssertions`]) and per-channel publishes ([`PublishedAssertions`]).
//!
//! Every method asserts (panicking with a descriptive message on failure) and returns `self`, so
//! checks chain: `subscriber("orders").assert_called_once().with(&Order { id: 1 }).settled(Ack)`.

// These methods run their check eagerly (panicking on failure) and return `self` only for optional
// chaining, so they are deliberately not `#[must_use]` (ending a chain on one is fine).
#![allow(clippy::return_self_not_must_use, clippy::must_use_candidate)]

use std::marker::PhantomData;

use crate::RawMessage;
use crate::runtime::HandlerResult;

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
    /// [`DefaultCodec`](crate::codec::DefaultCodec)) to `expected`.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, the payload fails to decode, or the decoded value
    /// differs from `expected`.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn with<T>(self, expected: &T) -> Self
    where
        T: serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        use crate::codec::Codec;
        self.with_last("the received value", |record| {
            let actual: T = crate::codec::DefaultCodec::default()
                .decode(&record.raw)
                .unwrap_or_else(|err| {
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
    T: serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    /// Asserts the most recent published payload decodes (with
    /// [`DefaultCodec`](crate::codec::DefaultCodec)) to `expected`.
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, the payload fails to decode, or it differs from `expected`.
    pub fn with(self, expected: &T) -> Self {
        use crate::codec::Codec;
        let actual: T = crate::codec::DefaultCodec::default()
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
