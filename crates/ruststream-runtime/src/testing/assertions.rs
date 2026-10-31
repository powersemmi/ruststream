//! Assertion builders over what the harness recorded: per-subscriber deliveries
//! ([`SubscriberAssertions`]) and per-channel publishes ([`PublishedAssertions`]).
//!
//! Every method asserts (panicking with a descriptive message on failure) and returns `self`, so
//! checks chain: `subscriber("orders").assert_called_once().with(&Order { id: 1 }).settled(Ack)`.

// These methods run their check eagerly (panicking on failure) and return `self` only for optional
// chaining, so ending a chain on one is fine and none of them is `#[must_use]`.
#![allow(clippy::return_self_not_must_use, clippy::must_use_candidate)]

use alloc::string::String;
use alloc::vec::Vec;
use core::any;

use bytes::Bytes;

use crate::HandlerOutcome;
use crate::codec::Codec;
// Only the DEFAULT codec is feature-dependent; the assertions that take a codec are always here.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use core::fmt::Debug;
use serde::de::DeserializeOwned;

use super::coordinator::{Coordinator, Delivered, Outcome, Record};
use super::hooks::Within;

mod published;

pub use published::PublishedAssertions;

/// Assertions over the deliveries one subscriber received, recorded by the harness.
///
/// The unit these count is the handler CALL, not the message: a single-message handler is called
/// once per delivery, and a batch handler once per batch. So `assert_called_once` on a batch
/// subscription means one batch arrived, whatever its size, while
/// [`received_raw`](Self::received_raw) still lists every element of it. An element the decode
/// policy rejected before the body ran is settled by that policy and is not part of the batch the
/// handler was called with, so it does not appear here.
#[derive(Debug)]
pub struct SubscriberAssertions<'a> {
    coordinator: &'a Coordinator,
    scope_id: usize,
    name: String,
    within: Within,
}

impl<'a> SubscriberAssertions<'a> {
    #[doc(hidden)]
    pub const fn new(coordinator: &'a Coordinator, scope_id: usize, name: String) -> Self {
        Self::within(coordinator, scope_id, name, Within::ALL)
    }

    /// Assertions over the calls recorded up to `within`: what a hook reads, the record as it
    /// stood at its call.
    #[doc(hidden)]
    pub const fn within(
        coordinator: &'a Coordinator,
        scope_id: usize,
        name: String,
        within: Within,
    ) -> Self {
        Self {
            coordinator,
            scope_id,
            name,
            within,
        }
    }

    /// Runs `f` over the recorded deliveries to this subscriber, in delivery order.
    fn with_records<R>(&self, f: impl FnOnce(&[&Record]) -> R) -> R {
        self.coordinator
            .with_records(self.scope_id, &self.name, self.within, f)
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
    /// expected payload go through here, so a batch of several reports what it was instead of
    /// silently checking one element of it.
    fn with_sole_delivery<R>(&self, what: &str, f: impl FnOnce(&Delivered) -> R) -> R {
        self.with_last(what, |record| match record.deliveries.as_slice() {
            [only] => f(only),
            batch => panic!(
                "subscriber {:?} last received a batch of {} deliveries, so it has no single \
                 {what}; read the batch with received_raw()",
                self.name,
                batch.len(),
            ),
        })
    }

    /// Asserts this subscriber was called exactly once (one delivery, or one batch).
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

    /// Asserts this subscriber was called exactly `times` times: once per delivery, each
    /// redelivery included, and once per batch.
    ///
    /// # Panics
    ///
    /// Panics if the call count differs from `times`.
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
    /// the built-in assertions. A batch contributes its elements, so the list is flat whether the
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

    /// The raw payloads this subscriber received grouped by handler CALL, in call order: one inner
    /// vector per call, holding what that call carried.
    ///
    /// This is [`received_raw`](Self::received_raw) with the batch boundaries kept. A
    /// single-message handler yields one-element vectors; a batch handler yields one vector per
    /// batch, which is what a test pinning how a stream was cut into batches reads.
    #[must_use]
    pub fn batches_raw(&self) -> Vec<Vec<Bytes>> {
        self.with_records(|records| {
            records
                .iter()
                .map(|record| {
                    record
                        .deliveries
                        .iter()
                        .map(|one| one.raw.clone())
                        .collect()
                })
                .collect()
        })
    }

    /// Decodes [`batches_raw`](Self::batches_raw) with
    /// [`DefaultCodec`](crate::codec::DefaultCodec). Use
    /// [`batches_with`](Self::batches_with) for a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if any received payload fails to decode as `T`.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    #[must_use]
    pub fn batches<T: DeserializeOwned>(&self) -> Vec<Vec<T>> {
        self.batches_with(&DefaultCodec::default())
    }

    /// Like [`batches`](Self::batches), but decodes with `codec` - use it when the handler was
    /// mounted with a non-default codec.
    ///
    /// # Panics
    ///
    /// Panics if any received payload fails to decode as `T`.
    #[must_use]
    pub fn batches_with<T, C>(&self, codec: &C) -> Vec<Vec<T>>
    where
        T: DeserializeOwned,
        C: Codec,
    {
        self.with_records(|records| {
            records
                .iter()
                .map(|record| {
                    record
                        .deliveries
                        .iter()
                        .map(|one| {
                            codec.decode(&one.raw).unwrap_or_else(|err| {
                                panic!(
                                    "subscriber {:?} received a payload that did not decode as \
                                     {}: {err}",
                                    self.name,
                                    any::type_name::<T>(),
                                )
                            })
                        })
                        .collect()
                })
                .collect()
        })
    }

    /// The classified [`Outcome`] of every call, in call order.
    ///
    /// [`assert_outcome`](Self::assert_outcome) and [`settled`](Self::settled) read the most recent
    /// call only; this is what a test pinning a SEQUENCE (a nack followed by the redelivery's ack)
    /// compares against.
    #[must_use]
    pub fn outcomes(&self) -> Vec<Outcome> {
        self.with_records(|records| records.iter().map(|record| record.outcome()).collect())
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
                            any::type_name::<T>(),
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
                    any::type_name::<T>(),
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

    /// Asserts the most recent delivery lent the handler `expected`: the value a broker's
    /// delivery already holds, on the carried lane ([`Carries`](crate::Carries)). That value never
    /// was bytes, so [`with`](Self::with) has nothing to decode; the harness keeps a clone of it
    /// instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "testing")]
    /// # mod demo {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::HandlerOutcome;
    /// use ruststream::testing::TestApp;
    ///
    /// /// A row a broker's driver reads, with the fields a test compares.
    /// #[derive(Debug, Clone, PartialEq)]
    /// pub struct Row {
    ///     pub id: u64,
    /// }
    ///
    /// pub fn the_row_reached_the_handler(tb: &TestApp<()>) {
    ///     tb.broker::<MemoryBroker>()
    ///         .subscriber("rows")
    ///         .assert_called_once()
    ///         .with_value(&Row { id: 7 })
    ///         .settled(HandlerOutcome::ack());
    /// }
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, the most recent call was a batch of several
    /// deliveries, its delivery lent no `T` (the handler takes no carried value, or the delivery
    /// carried none), or the value differs from `expected`.
    // `Clone` is the carried lane's own bound: what was recorded is a clone of a `T`.
    #[allow(clippy::extra_unused_type_parameters)]
    pub fn with_value<T>(self, expected: &T) -> Self
    where
        T: PartialEq + Debug + Clone + 'static,
    {
        self.with_sole_delivery("lent value", |one| {
            let actual = recorded::<T>(one, &self.name);
            assert_eq!(
                actual, expected,
                "subscriber {:?} was lent an unexpected value",
                self.name
            );
        });
        self
    }

    /// Every value this subscriber's deliveries lent it on the carried lane, in delivery order: the
    /// counterpart of [`received`](Self::received) for a value that never was bytes. A batch
    /// contributes its elements, so the list is flat whether the handler takes one value or a
    /// slice.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "testing")]
    /// # mod demo {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::HandlerOutcome;
    /// use ruststream::testing::TestApp;
    ///
    /// /// A row a broker's driver reads, with the fields a test compares.
    /// #[derive(Debug, Clone, PartialEq)]
    /// pub struct Row {
    ///     pub id: u64,
    /// }
    ///
    /// pub fn the_page_reached_the_handler(tb: &TestApp<()>) {
    ///     let broker = tb.broker::<MemoryBroker>();
    ///     assert_eq!(
    ///         broker.subscriber("rows").received_values::<Row>(),
    ///         [Row { id: 7 }, Row { id: 8 }],
    ///     );
    ///     broker
    ///         .subscriber("rows")
    ///         .assert_batch_sizes(&[2])
    ///         .settled(HandlerOutcome::ack());
    /// }
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if a delivery lent no `T`: the handler takes no carried value, or a delivery
    /// carried none and was settled by the decode policy.
    #[must_use]
    pub fn received_values<T: Clone + 'static>(&self) -> Vec<T> {
        self.with_records(|records| {
            records
                .iter()
                .flat_map(|record| record.deliveries.iter())
                .map(|one| recorded::<T>(one, &self.name).clone())
                .collect()
        })
    }

    /// Asserts the most recent call carried one delivery whose raw payload equals `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, the raw payload differs, or the most recent call
    /// was a batch of several deliveries (which has no single payload - read it with
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
    /// expected value is ignored: the harness compares the settlement itself).
    ///
    /// The settlement recorded is the one the dispatcher applied to the delivery, which is what
    /// the handler answered. A transport that cannot acknowledge refuses it with
    /// [`AckError::Unsupported`](crate::AckError::Unsupported) and the dispatcher logs the refusal,
    /// so a requeue such a transport cannot perform still reads back here as a requeue; what shows
    /// the difference is the redelivery it does not produce, counted by
    /// [`assert_called`](Self::assert_called).
    ///
    /// A batch settles per element, so this asserts EVERY element of the most recent batch settled
    /// that way - which is what a uniform answer (`HandlerOutcome::ack()` for the whole slice)
    /// produces. A batch that answered element by element with differing outcomes matches none.
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

    /// Asserts the body was handed batches of exactly `sizes`, in the order they arrived.
    ///
    /// The batch the body sees is the batch the broker delivered: the mount site names the size
    /// with [`batch`](crate::SubscriberSettings::batch) and the broker builds the batches
    /// to it, so this is where a test reads back that it did. One call is one batch, which is
    /// what [`assert_called`](Self::assert_called) counts, while
    /// [`received_raw`](Self::received_raw) still lists the elements flat. A single-message
    /// handler is called per delivery, so a run of three reports `[1, 1, 1]`.
    ///
    /// # Panics
    ///
    /// Panics if the batches differ from `sizes`.
    pub fn assert_batch_sizes(self, sizes: &[usize]) -> Self {
        self.with_records(|records| {
            let batches: Vec<usize> = records.iter().map(|r| r.deliveries.len()).collect();
            assert_eq!(
                batches.as_slice(),
                sizes,
                "subscriber {:?} was handed batches of {batches:?}",
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

    /// Asserts the most recent delivery did not decode into the handler's input type, by the
    /// codec's verdict or by its broker's report.
    ///
    /// # Panics
    ///
    /// Panics if the subscriber was not called, or the delivery decoded successfully.
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

/// The value one delivery lent its handler, as a `T`.
///
/// # Panics
///
/// Panics when the delivery lent nothing, or lent a value of another type.
fn recorded<'a, T: 'static>(one: &'a Delivered, subscriber: &str) -> &'a T {
    let value = one.value.as_ref().unwrap_or_else(|| {
        panic!(
            "subscriber {subscriber:?} was lent no value: its handler takes no carried `{}`, or \
             the delivery carried none",
            any::type_name::<T>(),
        )
    });
    value.downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "subscriber {subscriber:?} was lent a value that is not a `{}`",
            any::type_name::<T>(),
        )
    })
}

impl SubscriberAssertions<'_> {
    /// Runs `hook` each time this subscription's handler is called from now on: once per
    /// delivery, or once per batch.
    ///
    /// The hook runs when the harness records the call, after the delivery settled. It is handed
    /// this same handle, borrowed for the duration of the call, so it reads the record as it
    /// stood at that call, the call included and the calls recorded after it left out, with the
    /// assertions a test already uses
    /// (`assert_called(n)`, `with(..)`, `settled(..)`). Several hooks on one subscription run in
    /// the order they were registered; a call recorded before the registration does not run it.
    /// Both [`TestApp::start`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.start) and
    /// [`TestApp::start_live`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.start_live) run hooks.
    ///
    /// The harness awaits the hooks of a call before the publish that caused it returns, so a hook
    /// that never completes hangs that publish; the test's own timeout is the guard. A hook still
    /// running when the harness shuts down or is dropped is stopped where it waits, and `shutdown`
    /// returns once it is gone. On the tokio runtime a hook runs on a thread of its own inside the
    /// test's runtime, its clock included; on the embedded runtime it runs inside the test's own
    /// future, polled beside the app by the step that drove the call, and its shutdown moves the
    /// clock until the app has stopped, so a hook that waits on a timer runs to its end. A panic inside it, a failed assertion included, is raised again from the
    /// publish, `settle` or `advance` that drove the call, or else from [`TestApp::shutdown`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.shutdown) or the
    /// harness being dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use ruststream::memory::prelude::*;
    /// use ruststream::testing::TestApp;
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
    /// let calls = Arc::new(AtomicUsize::new(0));
    /// let seen = Arc::clone(&calls);
    /// tb.broker::<MemoryBroker>()
    ///     .subscriber("orders")
    ///     .on_call(async move |orders| {
    ///         seen.fetch_add(1, Ordering::SeqCst);
    ///         orders.settled(HandlerOutcome::ack());
    ///     });
    ///
    /// tb.broker::<MemoryBroker>()
    ///     .message(&Order { id: 7 })
    ///     .publish()
    ///     .await?;
    /// assert_eq!(calls.load(Ordering::SeqCst), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn on_call<Hook>(self, hook: Hook)
    where
        Hook: AsyncFn(SubscriberAssertions<'_>) + Send + Sync + 'static,
    {
        self.coordinator
            .hooks()
            .on_call(self.scope_id, self.name, hook);
    }
}
