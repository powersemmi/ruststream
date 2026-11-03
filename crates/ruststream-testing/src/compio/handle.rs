//! [`BrokerHandle`], the per-broker handle of a compio [`TestApp`]: where a test's input enters a
//! broker, and where the assertions on that broker's handlers and publishes start.

use std::fmt;
use std::future::poll_fn;
use std::pin::pin;
use std::task::Poll;

use ruststream_core::{Lend, OutgoingDestination, OutgoingMessage, RawMessage};
use ruststream_runtime::testing::{
    Coordinator, PublishTarget, PublishedAssertions, SubscriberAssertions, TestError,
    TestableBroker, attribute,
};
use ruststream_runtime::{
    HeadersUnset, MessageBody, PublishBuilder, PublishSink, SinkOutcome, UnnamedCodec, message_of,
};
use ruststream_runtime_compio::testing::Runnable;

use super::hooks::HookTasks;

/// Polls `future` once and drops it: the hand-over of a hook is its first poll.
async fn poll_once<Fut: Future>(future: Fut) {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let _ = future.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
}

/// One broker of a compio [`TestApp`](super::TestApp), addressed by its production type with
/// [`TestApp::broker`](super::TestApp::broker).
///
/// A test publishes its input through [`message`](Self::message), and reads what the broker's
/// subscriptions handled through [`subscriber`](Self::subscriber) and what reached a destination
/// through [`published`](Self::published).
pub struct BrokerHandle<'a> {
    pub(super) coordinator: &'a Coordinator,
    pub(super) hooks: HookTasks,
    pub(super) runnable: Runnable,
    pub(super) scope_id: usize,
    pub(super) broker: &'a dyn TestableBroker,
    pub(super) name: &'static str,
}

impl fmt::Debug for BrokerHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerHandle")
            .field("broker", &self.name)
            .finish_non_exhaustive()
    }
}

/// Where a test's publish enters the broker: the in-process transport, as an external producer
/// would publish. After the broker takes the message, the publish lets the app handle it.
pub struct InjectSink<'a> {
    coordinator: &'a Coordinator,
    hooks: HookTasks,
    runnable: Runnable,
    scope_id: usize,
    broker: &'a dyn TestableBroker,
}

impl fmt::Debug for InjectSink<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InjectSink").finish_non_exhaustive()
    }
}

impl PublishSink for InjectSink<'_> {
    // The in-process transport copies what it keeps out of the bytes it is lent.
    type Payload = Lend;
    type Error = TestError;
    // An injection stands in for an external producer, which reaches no broker's options type.
    type Options = ();

    async fn send(
        &mut self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&Self::Options>,
    ) -> SinkOutcome<Self::Error> {
        // The test's own publish is part of the record: it tells a message the test put on a
        // destination from one the app published there.
        let recorded = RawMessage::new(msg.name().to_owned(), msg.payload().to_vec())
            .with_headers(msg.headers().clone());
        let started = self.coordinator.test_publish_started();
        self.broker.inject(msg);
        let hooks = self
            .coordinator
            .record_published(self.scope_id, recorded, started)
            .run();
        // One poll hands the hooks of the test's own publish to the harness, which starts them
        // from `settled`: waiting for them here would wait for a step that only this publish
        // takes. A hook that waits on the clock stays queued past this publish, as one registered
        // on a subscriber does, and finishes when the test advances the clock or shuts the app
        // down.
        poll_once(hooks).await;
        super::settled(self.coordinator, &self.runnable, &self.hooks)
            .await
            .map_err(Into::into)
    }
}

impl<'a> BrokerHandle<'a> {
    /// Starts a publish of `value` onto this broker, as an external producer would publish it;
    /// the publish lets the app handle it before it returns.
    ///
    /// The destination is the type's `#[outgoing(name = ..)]`, or the builder's `.to(name)`.
    pub fn message<'v, T>(
        &self,
        value: &'v T,
    ) -> PublishBuilder<InjectSink<'a>, MessageBody<'v, T>, UnnamedCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        let sink = InjectSink {
            coordinator: self.coordinator,
            hooks: self.hooks.clone(),
            runnable: self.runnable.clone(),
            scope_id: self.scope_id,
            broker: self.broker,
        };
        message_of(sink, value, UnnamedCodec::new())
    }

    /// The assertions on what this broker's subscription `name` handled.
    #[must_use]
    pub fn subscriber(&self, name: &str) -> SubscriberAssertions<'_> {
        SubscriberAssertions::new(self.coordinator, self.scope_id, name.to_owned())
    }

    /// The assertions on what reached the destination `name` on this broker: the test's
    /// publishes and the app's, in the order the broker took them.
    #[must_use]
    pub fn published<T>(&self, name: &str) -> PublishedAssertions<T> {
        let recorded = self.coordinator.published(self.scope_id, name);
        let messages = self.broker.published(name);
        let options = attribute(&messages, &recorded, self.broker.evicted(name));
        let target = PublishTarget::Channel {
            scope_id: self.scope_id,
            name: name.to_owned(),
        };
        PublishedAssertions::new(self.coordinator, target, name.to_owned(), messages, options)
    }
}
