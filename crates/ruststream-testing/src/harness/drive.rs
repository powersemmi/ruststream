//! Driving the app under test and reading what it did: addressing a broker, publishing a test's
//! input, settling, advancing the clock, and asserting the run state.

use std::any::{TypeId, type_name};
use std::ptr;
use std::time::Duration;

use tokio::task;
use tokio::time;

use ruststream_core::OutgoingDestination;
use ruststream_core::{Broker, DefaultPublish, PublishPolicy, Publisher};
use ruststream_runtime::message_of;
use ruststream_runtime::{HeadersUnset, MessageBody, OutSlot, PublishBuilder, UnnamedCodec};
use ruststream_runtime_tokio::RustStreamError;

use ruststream_runtime::testing::{
    LivePublish, PublishedAssertions, TestError, TestableRegistration, live_publish,
};

use super::{BrokerEntry, Mode, TestApp};
use crate::handle::{BrokerHandle, Harness, InjectSink, Target, Transport};
use ruststream_runtime::testing::Within;

impl<State: Send + Sync + 'static> TestApp<State> {
    /// Addresses the unique broker of type `B`, the production type the app was built on.
    ///
    /// Live, a test's input reaches the broker through the connected form's
    /// [`DefaultPublish`] policy, which is what the bound asks for.
    ///
    /// # Panics
    ///
    /// Panics if no broker of type `B` is registered, or more than one is (address by label with
    /// [`broker_named`](Self::broker_named) instead).
    #[must_use]
    pub fn broker<B>(&self) -> BrokerHandle<'_>
    where
        B: Broker + 'static,
        B::Connected: DefaultPublish,
        <<B::Connected as DefaultPublish>::Policy as PublishPolicy<B::Connected>>::Live: Publisher,
    {
        let mut matches = self
            .entries
            .iter()
            .filter(|e| e.lifecycle.broker_type() == TypeId::of::<B>());
        let first = matches
            .next()
            .unwrap_or_else(|| panic!("no registered broker of type {}", type_name::<B>()));
        assert!(
            matches.next().is_none(),
            "more than one broker of type {} is registered; address one with broker_named(label)",
            type_name::<B>(),
        );
        self.handle(first, Some(live_publish::<B>))
    }

    /// Addresses the broker registered under `label` (see
    /// [`with_broker_labeled`](ruststream_runtime_tokio::RustStream::with_broker_labeled)).
    ///
    /// # Panics
    ///
    /// Panics if no broker carries `label`.
    #[must_use]
    pub fn broker_named(&self, label: &str) -> BrokerHandle<'_> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.label.as_deref() == Some(label))
            .unwrap_or_else(|| panic!("no broker labeled {label:?}"));
        self.handle(entry, None)
    }

    /// Asserts on what was published through the [`Out`](ruststream_runtime::Out) slot marked `M`:
    /// exactly the messages the handler sent through that injected publisher, with their
    /// destinations and headers, across all brokers.
    ///
    /// The untyped assertions ([`assert_called_once`](PublishedAssertions::assert_called_once),
    /// [`with_raw`](PublishedAssertions::with_raw), ...) apply directly; decode the payloads
    /// with [`decoded_as`](PublishedAssertions::decoded_as) for the typed
    /// [`with`](PublishedAssertions::with) form.
    ///
    /// Publishes made outside the handler task (a spawned sibling task, a settled owned
    /// transaction's buffer) are not attributed to the slot.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
    ///
    /// #[derive(Deserialized)]
    /// struct Chunk<'a>(&'a [u8]);
    ///
    /// // The frame on the way out: a serialized type carries its own bytes, so what the
    /// // handler republishes is byte-for-byte what it received.
    /// #[derive(Outgoing, Serialized)]
    /// struct Frame(Vec<u8>);
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Frame)]
    /// struct Encoded;
    ///
    /// #[subscriber("chunks")]
    /// async fn transcode(
    ///     chunk: &Chunk<'_>,
    ///     Out(out): Out<impl Publisher, Encoded>,
    /// ) -> HandlerOutcome {
    ///     let frame = Frame(chunk.0.to_vec());
    ///     if out.message(&frame).to("encoded").publish().await.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         b.include(transcode).out(Encoded, MemoryPublish).build();
    ///     });
    /// let tb = TestApp::start(app).await?;
    /// tb.broker::<MemoryBroker>()
    ///     .message(&Frame(b"frame".to_vec()))
    ///     .to("chunks")
    ///     .publish()
    ///     .await?;
    /// tb.out::<Encoded>().assert_called_once().with_raw(b"frame");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn out<M: OutSlot>(&self) -> PublishedAssertions<()> {
        PublishedAssertions::captured(&self.coordinator, M::NAME, Within::ALL)
    }

    /// The handle of `entry`. `typed` is how a live injection reaches it when the caller named the
    /// broker type; an untyped handle falls back on the type's registration.
    fn handle<'a>(
        &'a self,
        entry: &'a BrokerEntry,
        typed: Option<LivePublish>,
    ) -> BrokerHandle<'a> {
        let scope_id = self
            .entries
            .iter()
            .position(|e| ptr::eq(e, entry))
            .expect("entry belongs to this app");
        let transport = match &self.mode {
            Mode::InProcess => Transport::InProcess(
                entry
                    .testable()
                    .expect("an in-process broker connected through its registration"),
            ),
            Mode::Live { .. } => Transport::Live {
                connected: entry.lifecycle.as_any(),
                publish: typed.or_else(|| entry.registration.map(TestableRegistration::live)),
            },
        };
        BrokerHandle {
            harness: Harness {
                coordinator: &self.coordinator,
                shutdown: &self.shutdown,
                mode: &self.mode,
                brokers: &self.entries,
            },
            scope_id,
            transport,
            label: entry.display(),
        }
    }

    /// Publishes to the only registered broker, a convenience for single-broker apps.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Ambiguous`] when more than one broker is registered (use
    /// [`broker`](Self::broker) / [`broker_named`](Self::broker_named)), or any error from
    /// [`BrokerHandle::publish`].
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub async fn publish<T: serde::Serialize + Sync>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), TestError> {
        if self.entries.len() != 1 {
            return Err(TestError::Ambiguous);
        }
        self.handle(&self.entries[0], None)
            .publish(name, value)
            .await
    }

    /// Starts a typed injection on the only registered broker, a convenience for single-broker
    /// apps: `tb.message(&order).to("orders").publish().await?`.
    ///
    /// The scoped [`BrokerHandle::message`] with the broker chosen for you. An app registering
    /// more than one broker has no single target, so the publish reports
    /// [`TestError::Ambiguous`] and the test addresses a broker with [`broker`](Self::broker) /
    /// [`broker_named`](Self::broker_named) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
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
    /// tb.message(&Order { id: 7 }).publish().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<InjectSink<'a>, MessageBody<'a, T>, UnnamedCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        message_of(self.sole_sink(), value, UnnamedCodec::new())
    }

    /// The sole broker's sink, or the ambiguous one when the app registered more than one.
    fn sole_sink(&self) -> InjectSink<'_> {
        match self.entries.as_slice() {
            [only] => self.handle(only, None).sink(),
            _ => InjectSink(Target::Ambiguous),
        }
    }

    /// Drives any in-flight reaction to a standstill (handlers run, their publishes cascade) without
    /// publishing anything new. A publish through the harness calls this for you; use it after a
    /// reaction the test started some other way.
    ///
    /// In process it waits for the transports to report nothing in flight. Live, where nothing in
    /// this process can read the broker's queues, it waits until every subscription has handled
    /// what the harness saw published to it and every redelivery that fell due, and no handler is
    /// running, up to the deadline the harness was started with.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] if an in-process reaction does not settle within the
    /// step budget, or [`TestError::NotSettled`] naming the subscription a live reaction left
    /// unsettled at the deadline.
    pub async fn settle(&self) -> Result<(), TestError> {
        self.mode.settle(&self.coordinator, &self.entries).await
    }

    /// Lets `by` pass, fires every `nack_after` / `retry_after` redelivery now due, and drives the
    /// resulting reaction to a standstill. Use it to test delayed redeliveries: `publish` records
    /// the immediate `NackAfter` settlement and returns; `advance` then delivers the message
    /// again. A broker's native delayed redelivery and the runtime's deferred re-publish both
    /// arrive this way, in either mode; the copy the runtime publishes for a zero delay does not
    /// wait for it.
    ///
    /// In process it moves the paused clock by `by`, so an in-process test runs on a paused clock
    /// (`#[tokio::test(start_paused = true)]` or `tokio::time::pause`; on a running clock
    /// `tokio::time::advance` panics). Live it lets `by` of real time pass, then settles, waiting
    /// for every redelivery that fell due by then.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] if an in-process reaction does not settle within the
    /// step budget, or [`TestError::NotSettled`] naming the subscription a live reaction left
    /// unsettled at the deadline.
    pub async fn advance(&self, by: Duration) -> Result<(), TestError> {
        match self.mode {
            Mode::InProcess => time::advance(by).await,
            // Why real time passes here: the delay belongs to the real broker's own timer, which a
            // paused clock in this process does not reach.
            Mode::Live { .. } => time::sleep(by).await,
        }
        self.coordinator.fire_due_timers().await;
        self.settle().await
    }

    /// Waits for the post-settle continuations of everything settled so far to finish, for tests
    /// that assert on their side effects. Synchronous handler effects need only
    /// [`settle`](Self::settle).
    ///
    /// A settlement registers its continuations - the outcome's `and_after` and the context's
    /// `after(..)` / `after_settle(..)` hooks alike - before it releases the delivery, so an
    /// injection that has returned has already put its continuations here for this to find.
    pub async fn drain(&self) {
        while !self.continuations.is_empty() {
            task::yield_now().await;
        }
    }

    /// The result the real [`run`](ruststream_runtime_tokio::RustStream::run) would return: `Ok` while the
    /// service is healthy, or [`RustStreamError::Dispatch`] once a fail-fast failure tore it down.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError::Dispatch`] when a handler panic (or a fail-fast decode failure)
    /// triggered shutdown.
    pub fn run_result(&self) -> Result<(), RustStreamError> {
        self.shutdown
            .peek_failure()
            .map_or(Ok(()), |reason| Err(RustStreamError::Dispatch(reason)))
    }

    /// Asserts the service is still running (no fail-fast shutdown was triggered).
    ///
    /// # Panics
    ///
    /// Panics if a fail-fast failure has torn the service down.
    pub fn assert_running(&self) {
        assert!(
            !self.shutdown.is_cancelled(),
            "expected the service to be running, but it was shut down: {:?}",
            self.shutdown.peek_failure(),
        );
    }

    /// Asserts a fail-fast failure has shut the service down.
    ///
    /// # Panics
    ///
    /// Panics if the service is still running.
    pub fn assert_shut_down(&self) {
        assert!(
            self.shutdown.is_cancelled(),
            "expected the service to be shut down, but it was still running",
        );
    }
}
