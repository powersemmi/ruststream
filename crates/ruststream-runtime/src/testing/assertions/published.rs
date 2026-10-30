//! Assertions over what was published to one channel ([`PublishedAssertions`]).

// These methods run their check eagerly (panicking on failure) and return `self` only for optional
// chaining, so ending a chain on one is fine and none of them is `#[must_use]`.
#![allow(clippy::return_self_not_must_use, clippy::must_use_candidate)]

use std::any;
use std::marker::PhantomData;

use crate::RawMessage;
use crate::codec::Codec;
// Only the DEFAULT codec is feature-dependent; the assertions that take a codec are always here.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

use crate::testing::attribution::split_record;
use crate::testing::attribution::{MessageOptions, RecordedOptions};
use crate::testing::coordinator::Coordinator;
use crate::testing::hooks::{PublishTarget, Within};

/// Assertions over what was published to one channel, read from the broker's publish log or from
/// an `Out` slot's view. The type parameter `T` is the expected payload type for
/// [`with`](Self::with).
#[derive(Debug)]
pub struct PublishedAssertions<T> {
    coordinator: Coordinator,
    target: PublishTarget,
    name: String,
    messages: Vec<RawMessage>,
    // One entry per message, aligned with `messages`: what the harness knows of the options that
    // message was published with.
    options: Vec<MessageOptions>,
    _payload: PhantomData<fn() -> T>,
}

impl<T> PublishedAssertions<T> {
    /// Assertions over a broker's publishes to one channel, with what the harness knows of each
    /// message's options.
    #[doc(hidden)]
    pub fn new(
        coordinator: &Coordinator,
        target: PublishTarget,
        name: String,
        messages: Vec<RawMessage>,
        options: Vec<MessageOptions>,
    ) -> Self {
        Self {
            coordinator: coordinator.clone(),
            target,
            name,
            messages,
            options,
            _payload: PhantomData,
        }
    }

    /// Assertions over an `Out` slot's view, where every publish also carries the options.
    #[doc(hidden)]
    pub fn captured(coordinator: &Coordinator, slot: &'static str, within: Within) -> Self {
        let (messages, options) = coordinator
            .slot_published(slot, within)
            .into_iter()
            .map(|record| (record.message, MessageOptions::Recorded(record.options)))
            .unzip();
        Self {
            coordinator: coordinator.clone(),
            target: PublishTarget::Slot(slot),
            name: format!("Out slot `{slot}`"),
            messages,
            options,
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

    /// Asserts exactly `times` messages were published to this channel.
    ///
    /// # Panics
    ///
    /// Panics if the published count differs from `times`.
    pub fn assert_called(self, times: usize) -> Self {
        let count = self.messages.len();
        assert_eq!(
            count, times,
            "channel {:?} was published to {count} times, expected {times}",
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
        PublishedAssertions {
            coordinator: self.coordinator,
            target: self.target,
            name: self.name,
            messages: self.messages,
            options: self.options,
            _payload: PhantomData,
        }
    }

    /// The options of the most recent publish, panicking if nothing was published or the harness
    /// cannot say which options it carried.
    fn last_options(&self, what: &str) -> &RecordedOptions {
        self.last(what);
        match self.options.last() {
            Some(MessageOptions::Recorded(options)) => options,
            Some(MessageOptions::Ambiguous) => panic!(
                "the last publish to {:?} has the same payload as another publish there, and the \
                 harness cannot tell which of them carried which options: they were in flight at \
                 once, or the harness never saw the other one; cannot assert {what}",
                self.name,
            ),
            Some(MessageOptions::Unrecorded) | None => panic!(
                "the harness recorded no per-message options for the last publish to {:?}: the \
                 test published it itself, a publisher the runtime did not pair made it, or the \
                 broker changed its payload; cannot assert {what}",
                self.name,
            ),
        }
    }

    /// Asserts the most recent publish carried exactly `expected` as the broker's per-message
    /// options.
    ///
    /// The options are what the broker's builder steps set on that one publish (see
    /// [`PublishBuilder::options_mut`](crate::PublishBuilder::options_mut)); a publish
    /// no step touched carried none, which
    /// [`assert_options_default`](Self::assert_options_default) asserts. The harness records
    /// them with every publish the runtime makes (a reply, an `Out` slot publish, a retry copy),
    /// on the broker it went to, so a broker's view and a slot view both read them.
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, the harness cannot say which options the most recent
    /// publish carried (the test published it itself, a publisher the runtime did not pair made
    /// it, or another publish there has the same payload and either the order of the two is
    /// unknown or the harness never saw that one),
    /// it carried none, the broker's options type is not `Options`, or the value differs from
    /// `expected`.
    pub fn with_options<Options>(self, expected: &Options) -> Self
    where
        Options: PartialEq + Debug + 'static,
    {
        let recorded = self.last_options("the per-message options");
        let actual = recorded.downcast::<Options>(&self.name).unwrap_or_else(|| {
            panic!(
                "channel {:?} published with the policy's default options, expected \
                     {expected:?}",
                self.name,
            )
        });
        assert_eq!(
            actual, expected,
            "channel {:?} published with unexpected per-message options",
            self.name,
        );
        self
    }

    /// Asserts the most recent publish carried no per-message options: no builder step ran on
    /// it, and the publish policy's own settings applied.
    ///
    /// Recorded for every publish the runtime makes, like [`with_options`](Self::with_options).
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, the harness cannot say which options the most recent
    /// publish carried (see [`with_options`](Self::with_options)), or a step did set options on
    /// the publish.
    pub fn assert_options_default(self) -> Self {
        let recorded = self.last_options("the per-message options");
        assert!(
            recorded.value.is_none(),
            "channel {:?} published with per-message options set (`{}`), expected the policy's \
             defaults",
            self.name,
            recorded.type_name,
        );
        self
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

    /// Asserts the most recent published message carries the header `name` with `value`.
    ///
    /// What a publish transform or an app-wide
    /// [`publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer) put on the message is read
    /// here: the assertion sees the message as it left, so a stamp added on the way out shows up
    /// on the reply channel and on `tb.out::<Marker>()` alike.
    ///
    /// # Panics
    ///
    /// Panics if nothing was published, the header is absent, or its value differs.
    pub fn with_header(self, name: &str, value: impl AsRef<[u8]>) -> Self {
        let message = self.last("the published headers");
        let actual = message.headers().get(name).unwrap_or_else(|| {
            panic!(
                "channel {:?} published a message without the {name:?} header",
                self.name,
            )
        });
        assert_eq!(
            actual,
            value.as_ref(),
            "channel {:?} published an unexpected {name:?} header",
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
                    any::type_name::<T>(),
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
                        any::type_name::<T>(),
                    )
                })
            })
            .collect()
    }
}

impl<T> PublishedAssertions<T> {
    /// Assertions over the harness's record of `target` up to `within`: what a hook on it is
    /// handed, the record as it stood at its publish.
    pub(crate) fn at(coordinator: &Coordinator, target: &PublishTarget, within: Within) -> Self {
        match target {
            PublishTarget::Channel { scope_id, name } => {
                let recorded = coordinator.published_within(*scope_id, name, within);
                let (messages, options) = split_record(recorded);
                Self::new(coordinator, target.clone(), name.clone(), messages, options)
            }
            PublishTarget::Slot(slot) => Self::captured(coordinator, slot, within),
        }
    }
}

impl<T: 'static> PublishedAssertions<T> {
    /// Runs `hook` each time a message is published to this destination from now on, through
    /// every path the harness records: the test's own publishes and every publish a publisher the
    /// runtime paired makes (replies, `Out` slot publishes and requests, retry copies, dead
    /// letters). On the view of an `Out` slot ([`TestApp::out`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.out)) it runs
    /// for each publish through that slot.
    ///
    /// The hook is handed assertions over this destination as the harness's record stood at that
    /// publish, the publish included and the ones recorded after it left out, with the surface a
    /// test already uses
    /// (`assert_called(n)`, `with(..)`, `decoded()`). The record is the message as the framework
    /// handed it to the broker, in both start modes. Several hooks on one destination run in the
    /// order they were registered; a publish recorded before the registration does not run it.
    ///
    /// The harness awaits the hooks of a publish before the test's publish that caused it
    /// returns, so a hook that never completes hangs that publish; the test's own timeout is the
    /// guard. A hook still running when the harness shuts down or is dropped is stopped where it
    /// waits, and `shutdown` returns once it is gone. A hook runs on a thread of its own inside
    /// the test's runtime, its clock included. A
    /// panic inside it, a failed assertion included, is raised again from the publish, `settle`
    /// or `advance` that drove the event, or else from
    /// [`TestApp::shutdown`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.shutdown) or the harness being dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::testing::TestApp;
    /// use serde::{Deserialize, Serialize};
    /// use tokio::sync::mpsc;
    ///
    /// #[derive(Outgoing, Serialize, Deserialize)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u32,
    /// }
    ///
    /// #[derive(Debug, Outgoing, Serialize, Deserialize)]
    /// #[outgoing(name = "confirmations")]
    /// struct Confirmation {
    ///     id: u32,
    /// }
    ///
    /// #[subscriber("orders", reply)]
    /// async fn confirm(order: &Order) -> Confirmation {
    ///     Confirmation { id: order.id }
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///     b.include(confirm).out_reply(Publish);
    /// });
    /// let tb = TestApp::start(app).await?;
    ///
    /// let (audit, mut confirmed) = mpsc::channel(8);
    /// tb.broker::<MemoryBroker>()
    ///     .published::<Confirmation>("confirmations")
    ///     .on_publish(async move |confirmations| {
    ///         let last = confirmations.decoded().pop();
    ///         let _ = audit.send(last).await;
    ///     });
    ///
    /// tb.broker::<MemoryBroker>()
    ///     .message(&Order { id: 7 })
    ///     .publish()
    ///     .await?;
    /// let confirmation = confirmed.recv().await.flatten().ok_or("no confirmation")?;
    /// assert_eq!(confirmation.id, 7);
    /// # Ok(())
    /// # }
    /// ```
    pub fn on_publish<Hook>(self, hook: Hook)
    where
        Hook: AsyncFn(Self) + Send + Sync + 'static,
    {
        let Self {
            coordinator,
            target,
            ..
        } = self;
        coordinator.hooks().on_publish(target, hook);
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
            &Coordinator::new(1),
            PublishTarget::Slot("orders"),
            "orders".to_owned(),
            vec![RawMessage::new("orders", b"not json".as_slice())],
            Vec::new(),
        )
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

    // A header assertion that fails has to name the header, or a stamping middleware's absence
    // reads as "some header differs".
    #[test]
    #[should_panic(expected = "without the \"x-app\" header")]
    fn a_missing_published_header_names_itself() {
        let bare = PublishedAssertions::<Order>::new(
            &Coordinator::new(1),
            PublishTarget::Slot("orders"),
            "orders".to_owned(),
            vec![RawMessage::new("orders", br#"{"id":7}"#.as_slice())],
            Vec::new(),
        );
        bare.with_header("x-app", b"1");
    }

    #[test]
    #[should_panic(expected = "was published to 1 times, expected 2")]
    fn a_wrong_published_count_reports_both_numbers() {
        let one = PublishedAssertions::<Order>::new(
            &Coordinator::new(1),
            PublishTarget::Slot("orders"),
            "orders".to_owned(),
            vec![RawMessage::new("orders", br#"{"id":7}"#.as_slice())],
            Vec::new(),
        );
        one.assert_called(2);
    }
}
