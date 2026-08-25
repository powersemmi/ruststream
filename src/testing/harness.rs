//! The [`TestApp`] harness: drives a built [`RustStream`](crate::runtime::RustStream) application in
//! process, with no server, and exposes per-broker assertions. Each registered broker's consuming
//! `connect` runs to produce the connected form the subscriptions need; for an in-process broker
//! that transition performs no I/O.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// The default-codec publish helpers are gated on a codec feature, like the codec itself.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::OutgoingDestination;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::{Codec, DefaultCodec};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::{CallCodec, MessageBody, message_of};
use crate::runtime::{
    ConnectedLifecycle, ErrorShutdown, HeadersUnset, LifecycleHook, OutSlot, Publish,
    PublishIdentity, PublishSink, RawBody, RegisteredBroker, RustStream, RustStreamError, Starter,
    TestParts, raw_of,
};
use crate::{CallerName, OutgoingMessage};

use super::assertions::{PublishedAssertions, SubscriberAssertions};
use super::broker::{TestableBroker, TestableRegistration};
use super::coordinator::Coordinator;

/// The default cap on dispatched deliveries before [`TestApp::publish`] gives up driving a reaction
/// to quiescence. Guards against a non-terminating requeue loop.
const DEFAULT_MAX_STEPS: usize = 10_000;

/// An error from the test harness.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TestError {
    /// An `on_startup` or `after_startup` lifecycle hook failed while starting the harness.
    #[error("startup hook failed: {0}")]
    Startup(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A subscription failed to open while starting the harness.
    #[error("subscription failed: {0}")]
    Subscribe(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A reaction did not settle within the step budget (a non-terminating requeue?).
    #[error("the reaction did not settle within {processed} dispatched deliveries")]
    NotQuiescent {
        /// How many deliveries were dispatched before the harness gave up.
        processed: usize,
    },
    /// A broker failed its consuming `connect` while starting the harness.
    #[error("broker {broker} failed to connect: {source}")]
    Connect {
        /// The broker's label, or its registration index for unlabeled brokers.
        broker: String,
        /// The broker's own connect error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A publish was attempted after a fail-fast failure tore the service down.
    #[error("publish after the service shut down")]
    ShutDown,
    /// An unscoped [`TestApp::publish`] is ambiguous: more than one broker is registered.
    #[error("more than one broker is registered; address one with broker::<B>() or broker_named()")]
    Ambiguous,
    /// The addressed broker has no in-process test transport (it does not implement
    /// [`TestableBroker`](super::TestableBroker), or its feature is disabled).
    #[error("broker {0} has no in-process test transport")]
    NoTransport(String),
    /// The message failed to encode for publishing.
    #[error("failed to encode the message: {0}")]
    Encode(String),
}

/// One broker registered in the app under test: its label, its erased connected handle (for
/// type/label addressing), and the registration that recovers its [`TestableBroker`] view (when it
/// is registered with [`register_testable_broker!`](crate::register_testable_broker)).
struct BrokerEntry {
    label: Option<String>,
    lifecycle: Box<dyn ConnectedLifecycle>,
    registration: Option<&'static TestableRegistration>,
}

impl BrokerEntry {
    /// The broker's `TestableBroker` view, recovered from the erased handle via its registration.
    fn testable(&self) -> Option<&dyn TestableBroker> {
        self.registration
            .and_then(|registration| registration.resolve(self.lifecycle.as_any()))
    }

    /// The name used to address this broker in diagnostics: its label, else its broker type name.
    fn display(&self) -> String {
        self.label
            .clone()
            .unwrap_or_else(|| self.lifecycle.name().to_owned())
    }
}

/// Finds the [`TestableBroker`] registration matching this broker and installs the coordinator into
/// its bus. Returns `None` for a broker whose type was not registered with
/// [`register_testable_broker!`](crate::register_testable_broker).
fn recover_testable(
    // The explicit object bound keeps the default from shrinking to the reference lifetime,
    // which `as_any`'s `Self: 'static` requirement rejects.
    lifecycle: &(dyn ConnectedLifecycle + 'static),
    coordinator: &Coordinator,
) -> Option<&'static TestableRegistration> {
    let any = lifecycle.as_any();
    for registration in inventory::iter::<TestableRegistration> {
        if let Some(broker) = registration.resolve(any) {
            broker.install_coordinator(coordinator.clone());
            return Some(registration);
        }
    }
    None
}

/// Borrowed view of the app's brokers handed to a [`TestApp::with_state`] builder, so it can wire a
/// mirror state's publishers onto the same bus the assertions read.
pub struct TestBrokers<'a> {
    entries: &'a [BrokerEntry],
}

impl fmt::Debug for TestBrokers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestBrokers")
            .field("brokers", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl TestBrokers<'_> {
    /// Returns the connected form of the unique registered broker of type `B`, for building a
    /// mirror state's publishers (`tb.broker::<MemoryBroker>().publisher()`).
    ///
    /// # Panics
    ///
    /// Panics if no broker of type `B` is registered, or more than one is (disambiguate the app, or
    /// address by label is not supported when building state).
    #[must_use]
    pub fn broker<B: crate::Broker + 'static>(&self) -> &B::Connected {
        let mut found = self
            .entries
            .iter()
            .filter_map(|e| e.lifecycle.as_any().downcast_ref::<B::Connected>());
        let first = found.next().unwrap_or_else(|| {
            panic!(
                "no registered broker of type {}",
                std::any::type_name::<B>()
            )
        });
        assert!(
            found.next().is_none(),
            "more than one broker of type {} is registered",
            std::any::type_name::<B>(),
        );
        first
    }
}

/// In-process harness around a built application.
///
/// Drives input through the broker bus (no server; each broker's consuming `connect` runs, which
/// is I/O-free for in-process brokers), records what handlers saw and published, and exposes
/// per-broker assertions.
///
/// Build one with [`start`](Self::start) (runs the app's real `on_startup`) or
/// [`with_state`](Self::with_state) (injects a mirror state for non-broker dependencies). Drive
/// input with [`broker`](Self::broker) / [`broker_named`](Self::broker_named) and assert on what
/// happened.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "testing", feature = "memory", feature = "json"))]
/// # async fn demo() -> Result<(), ruststream::testing::TestError> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, Context, HandlerResult, RustStream};
/// use ruststream::subscriber;
/// use ruststream::testing::TestApp;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, PartialEq, Debug)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn handle(order: &Order) -> HandlerResult {
///     let _ = order;
///     HandlerResult::Ack
/// }
///
/// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
///     .with_broker(MemoryBroker::new(), |b| b.include(handle));
/// let tb = TestApp::start(app).await?;
///
/// tb.broker::<MemoryBroker>().publish("orders", &Order { id: 1 }).await?;
/// tb.broker::<MemoryBroker>()
///     .subscriber("orders")
///     .assert_called_once()
///     .with(&Order { id: 1 })
///     .settled(HandlerResult::Ack);
/// # Ok(())
/// # }
/// ```
pub struct TestApp<State> {
    entries: Vec<BrokerEntry>,
    coordinator: Coordinator,
    #[allow(dead_code)]
    state: Arc<State>,
    error_shutdown: ErrorShutdown,
    token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
}

impl<State> fmt::Debug for TestApp<State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestApp")
            .field("brokers", &self.entries.len())
            .field("subscribers", &self.handles.len())
            .finish_non_exhaustive()
    }
}

impl<State: Send + Sync + 'static> TestApp<State> {
    /// Starts the harness by running the app's real `on_startup` (the existing state and its
    /// publishers bind to the in-process bus). Each broker's consuming `connect` runs to produce
    /// the connected form; for an in-process broker that transition performs no I/O.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Startup`] if a lifecycle hook fails, [`TestError::Connect`] if a
    /// broker fails to connect, or [`TestError::Subscribe`] if a subscription fails to open.
    pub async fn start<Layers, Phase>(
        app: RustStream<Layers, State, PublishIdentity, Phase>,
    ) -> Result<Self, TestError> {
        let (coordinator, entries, parts) = Self::setup(app).await?;
        let TestParts {
            starters,
            state_init,
            after_startup,
            shutdown_timeout,
            continuations,
            ..
        } = parts;
        let state = state_init().await.map_err(TestError::Startup)?;
        Self::spawn(SpawnArgs {
            coordinator,
            entries,
            starters,
            after_startup,
            continuations,
            shutdown_timeout,
            state: Arc::new(state),
        })
        .await
    }

    /// Starts the harness with an injected mirror `state`, instead of running the app's
    /// `on_startup`. `build` receives the brokers so it can wire the mirror state's publishers onto
    /// the same bus (`tb.broker::<MemoryBroker>().publisher()`) and supply fakes for non-broker
    /// dependencies. Each broker's consuming `connect` runs first, so the mirror state's
    /// publishers pair against connected brokers.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Connect`] if a broker fails to connect, or [`TestError::Subscribe`]
    /// if a subscription fails to open.
    pub async fn with_state<Layers, F, Phase>(
        app: RustStream<Layers, State, PublishIdentity, Phase>,
        build: F,
    ) -> Result<Self, TestError>
    where
        F: FnOnce(&TestBrokers<'_>) -> State,
    {
        let (coordinator, entries, parts) = Self::setup(app).await?;
        let TestParts {
            starters,
            after_startup,
            shutdown_timeout,
            continuations,
            ..
        } = parts;
        let state = build(&TestBrokers { entries: &entries });
        Self::spawn(SpawnArgs {
            coordinator,
            entries,
            starters,
            after_startup,
            continuations,
            shutdown_timeout,
            state: Arc::new(state),
        })
        .await
    }

    /// Installs a fresh coordinator into the app's hooks slot, drives each broker's consuming
    /// `connect`, and recovers the per-broker transports from the connected forms. Returns the
    /// coordinator, the broker entries, and the remaining parts (the brokers field is now
    /// consumed and empty).
    async fn setup<Layers, Phase>(
        app: RustStream<Layers, State, PublishIdentity, Phase>,
    ) -> Result<(Coordinator, Vec<BrokerEntry>, TestParts<State>), TestError> {
        let mut parts = app.into_test_parts();
        let coordinator = Coordinator::new(DEFAULT_MAX_STEPS);
        parts.test_hooks.install(coordinator.clone());
        let mut entries = Vec::new();
        for (index, RegisteredBroker { lifecycle, label }) in
            std::mem::take(&mut parts.brokers).into_iter().enumerate()
        {
            // The unconnected erased handle has no type name to report, so the label (or the
            // registration index) is the identity available before connect succeeds.
            let broker = label.clone().unwrap_or_else(|| format!("#{index}"));
            let lifecycle = lifecycle
                .connect()
                .await
                .map_err(|source| TestError::Connect { broker, source })?;
            let registration = recover_testable(lifecycle.as_ref(), &coordinator);
            entries.push(BrokerEntry {
                label,
                lifecycle,
                registration,
            });
        }
        Ok((coordinator, entries, parts))
    }

    /// Spawns the dispatch loops against the (uninstalled) bus and runs `after_startup`, completing
    /// the harness. No broker `connect` runs.
    async fn spawn(args: SpawnArgs<State>) -> Result<Self, TestError> {
        let SpawnArgs {
            coordinator,
            entries,
            starters,
            after_startup,
            continuations,
            shutdown_timeout,
            state,
        } = args;
        let token = CancellationToken::new();
        let error_shutdown = ErrorShutdown::new(token.clone());
        let mut handles = Vec::with_capacity(starters.len());
        for starter in starters {
            let handle = starter(state.clone(), error_shutdown.clone(), token.clone())
                .await
                .map_err(TestError::Subscribe)?;
            handles.push(handle);
        }
        for hook in after_startup {
            hook(state.clone()).await.map_err(TestError::Startup)?;
        }
        Ok(Self {
            entries,
            coordinator,
            state,
            error_shutdown,
            token,
            handles,
            continuations,
            shutdown_timeout,
        })
    }

    /// Addresses the unique broker of type `B`.
    ///
    /// # Panics
    ///
    /// Panics if no broker of type `B` is registered, or more than one is (address by label with
    /// [`broker_named`](Self::broker_named) instead).
    #[must_use]
    pub fn broker<B: crate::Broker + 'static>(&self) -> BrokerHandle<'_> {
        let mut matches = self.entries.iter().filter(|e| {
            e.lifecycle
                .as_any()
                .downcast_ref::<B::Connected>()
                .is_some()
        });
        let first = matches.next().unwrap_or_else(|| {
            panic!(
                "no registered broker of type {}",
                std::any::type_name::<B>()
            )
        });
        assert!(
            matches.next().is_none(),
            "more than one broker of type {} is registered; address one with broker_named(label)",
            std::any::type_name::<B>(),
        );
        self.handle(first)
    }

    /// Addresses the broker registered under `label` (see
    /// [`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled)).
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
        self.handle(entry)
    }

    /// Asserts on what was published through the [`Out`](crate::runtime::Out) slot marked `M`:
    /// exactly the messages the handler sent through that injected publisher, with their
    /// destinations and headers, across all brokers.
    ///
    /// The untyped assertions ([`assert_called_once`](PublishedAssertions::assert_called_once),
    /// [`with_raw`](PublishedAssertions::with_raw), ...) apply directly; decode the payloads
    /// with [`decoded_as`](PublishedAssertions::decoded_as) for the typed
    /// [`with`](PublishedAssertions::with) form.
    ///
    /// Publishes made outside the handler task (a spawned sibling task, a settled owned
    /// transaction's buffer) are visible in the broker's publish log
    /// ([`BrokerHandle::published`]) but are not attributed to the slot.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, HandlerResult, Out, RustStream};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{OutSlot, Publisher, subscriber};
    ///
    /// #[derive(OutSlot)]
    /// struct Encoded;
    ///
    /// #[subscriber("chunks", raw)]
    /// async fn transcode(chunk: &[u8], Out(out): Out<impl Publisher, Encoded>) -> HandlerResult {
    ///     if out.raw(chunk).to("encoded").publish().await.is_err() {
    ///         return HandlerResult::retry();
    ///     }
    ///     HandlerResult::Ack
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         b.include(transcode).out(Encoded, MemoryPublish).mount();
    ///     });
    /// let tb = TestApp::start(app).await?;
    /// tb.broker::<MemoryBroker>().raw(b"frame").to("chunks").publish().await?;
    /// tb.out::<Encoded>().assert_called_once().with_raw(b"frame");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn out<M: OutSlot>(&self) -> PublishedAssertions<()> {
        PublishedAssertions::new(
            format!("Out slot `{}`", M::NAME),
            self.coordinator.slot_published(M::NAME),
        )
    }

    fn handle<'a>(&'a self, entry: &'a BrokerEntry) -> BrokerHandle<'a> {
        let scope_id = self
            .entries
            .iter()
            .position(|e| std::ptr::eq(e, entry))
            .expect("entry belongs to this app");
        BrokerHandle {
            scope_id,
            coordinator: &self.coordinator,
            testable: entry.testable(),
            token: &self.token,
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
        self.handle(&self.entries[0]).publish(name, value).await
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
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
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
    /// async fn handle(order: &Order) -> HandlerResult {
    ///     let _ = order.id;
    ///     HandlerResult::Ack
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| b.include(handle));
    /// let tb = TestApp::start(app).await?;
    ///
    /// tb.message(&Order { id: 7 }).publish().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> Publish<InjectSink<'a>, MessageBody<'a, T>, CallCodec<DefaultCodec>, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        message_of(self.sole_sink(), value, CallCodec(DefaultCodec::default()))
    }

    /// Starts a byte injection on the only registered broker, a convenience for single-broker
    /// apps: `tb.raw(b"frame").to("frames").publish().await?`.
    ///
    /// The scoped [`BrokerHandle::raw`] with the broker chosen for you; ambiguity is reported
    /// the same way [`message`](Self::message) reports it.
    pub fn raw<'a, B>(
        &'a self,
        payload: &'a B,
    ) -> Publish<InjectSink<'a>, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(self.sole_sink(), payload)
    }

    /// The sole broker's sink, or the ambiguous one when the app registered more than one.
    fn sole_sink(&self) -> InjectSink<'_> {
        match self.entries.as_slice() {
            [only] => self.handle(only).sink(),
            _ => InjectSink(Target::Ambiguous),
        }
    }

    /// Drives any in-flight reaction to a standstill (handlers run, their publishes cascade) without
    /// publishing anything new. [`BrokerHandle::publish`] calls this for you; use it after manually
    /// advancing time for a delayed redelivery.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] if the reaction does not settle within the step budget.
    pub async fn settle(&self) -> Result<(), TestError> {
        self.coordinator.drive().await
    }

    /// Advances the (paused) clock by `by`, fires every `nack_after` / `retry_after` redelivery now
    /// due, and drives the resulting reaction to a standstill. Use it to test delayed redeliveries:
    /// `publish` records the immediate `NackAfter` settlement and returns; `advance` then delivers
    /// the message again.
    ///
    /// Requires a paused clock (`#[tokio::test(start_paused = true)]` or `tokio::time::pause`); on a
    /// live clock `tokio::time::advance` panics.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] if the redelivered reaction does not settle within the
    /// step budget.
    pub async fn advance(&self, by: Duration) -> Result<(), TestError> {
        tokio::time::advance(by).await;
        self.coordinator.fire_due_timers().await;
        self.coordinator.drive().await
    }

    /// Waits (best-effort) for post-settle `and_after` continuations spawned so far to finish, for
    /// tests that assert on their side effects. Synchronous handler effects need only
    /// [`settle`](Self::settle).
    pub async fn drain(&self) {
        while !self.continuations.is_empty() {
            tokio::task::yield_now().await;
        }
    }

    /// The result the real [`run`](crate::runtime::RustStream::run) would return: `Ok` while the
    /// service is healthy, or [`RustStreamError::Dispatch`] once a fail-fast failure tore it down.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError::Dispatch`] when a handler panic (or a fail-fast decode failure)
    /// triggered shutdown.
    pub fn run_result(&self) -> Result<(), RustStreamError> {
        self.error_shutdown
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
            !self.token.is_cancelled(),
            "expected the service to be running, but it was shut down: {:?}",
            self.error_shutdown.peek_failure(),
        );
    }

    /// Asserts a fail-fast failure has shut the service down.
    ///
    /// # Panics
    ///
    /// Panics if the service is still running.
    pub fn assert_shut_down(&self) {
        assert!(
            self.token.is_cancelled(),
            "expected the service to be shut down, but it was still running",
        );
    }

    /// Shuts the harness down: stops the dispatch loops, drains in-flight handlers and post-settle
    /// continuations (bounded by the app's shutdown timeout), and returns [`run_result`](Self::run_result).
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError::Dispatch`] when a fail-fast failure tore the service down.
    pub async fn shutdown(self) -> Result<(), RustStreamError> {
        self.token.cancel();
        match self.shutdown_timeout {
            Some(timeout) => {
                for handle in self.handles {
                    let _ = tokio::time::timeout(timeout, handle).await;
                }
            }
            None => {
                for handle in self.handles {
                    let _ = handle.await;
                }
            }
        }
        self.continuations.close();
        self.continuations.wait().await;
        self.error_shutdown
            .taken_failure()
            .map_or(Ok(()), |reason| Err(RustStreamError::Dispatch(reason)))
    }
}

/// The pieces [`TestApp::spawn`] needs to start the dispatch loops.
struct SpawnArgs<State> {
    coordinator: Coordinator,
    entries: Vec<BrokerEntry>,
    starters: Vec<Starter<State>>,
    after_startup: Vec<LifecycleHook<State>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
    state: Arc<State>,
}

/// A handle to one broker in a [`TestApp`]: inject input and assert on its handlers and publishes.
pub struct BrokerHandle<'a> {
    scope_id: usize,
    coordinator: &'a Coordinator,
    testable: Option<&'a dyn TestableBroker>,
    token: &'a CancellationToken,
    label: String,
}

impl fmt::Debug for BrokerHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerHandle")
            .field("broker", &self.label)
            .field("testable", &self.testable.is_some())
            .finish_non_exhaustive()
    }
}

/// The publish builder's sink for the harness.
///
/// It injects the message onto a broker's in-process transport the way an external producer
/// would, then drives the resulting reaction to a standstill before the publish returns.
///
/// Produced by the `message(..)` and `raw(..)` entry points of [`TestApp`] and [`BrokerHandle`],
/// so a test injects through the same positions - destination, typed headers, codec - that the
/// service itself publishes through. You never name this type. Every other entry point of the
/// handle ends here too, so one place decides what an injection does.
pub struct InjectSink<'a>(Target<'a>);

/// What an [`InjectSink`] sends into: a resolved broker, or none because the unscoped entry
/// point had more than one to choose from.
///
/// The unscoped `message(..)` / `raw(..)` exist whatever the app registered, so the ambiguity
/// rides here and surfaces from the publish, keeping the error the caller already handles.
enum Target<'a> {
    Broker {
        coordinator: &'a Coordinator,
        testable: Option<&'a dyn TestableBroker>,
        token: &'a CancellationToken,
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
    type Error = TestError;

    async fn send(&mut self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let Target::Broker {
            coordinator,
            testable,
            token,
            label,
        } = &self.0
        else {
            return Err(TestError::Ambiguous);
        };
        if token.is_cancelled() {
            return Err(TestError::ShutDown);
        }
        let transport = testable.ok_or_else(|| TestError::NoTransport(label.clone()))?;
        transport.inject(msg);
        coordinator.drive().await
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
    /// use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
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
    /// async fn handle(order: &Order) -> HandlerResult {
    ///     let _ = order.id;
    ///     HandlerResult::Ack
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| b.include(handle));
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
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn message<'v, T>(
        &self,
        value: &'v T,
    ) -> Publish<InjectSink<'a>, MessageBody<'v, T>, CallCodec<DefaultCodec>, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        // The harness carries no codec of its own, so the crate default rides in the call
        // position - the same bottom of the ladder a bare publisher uses.
        message_of(self.sink(), value, CallCodec(DefaultCodec::default()))
    }

    /// Starts a byte injection onto this broker: the payload travels as it is, to the
    /// destination named with `to(..)`.
    ///
    /// The undecodable-payload path of a test, and the only one for a raw subscriber. Awaiting
    /// the publish drives the resulting reaction to a standstill.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
    /// use ruststream::subscriber;
    /// use ruststream::testing::TestApp;
    ///
    /// #[subscriber("frames", raw)]
    /// async fn handle(frame: &[u8]) -> HandlerResult {
    ///     let _ = frame.len();
    ///     HandlerResult::Ack
    /// }
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| b.include(handle));
    /// let tb = TestApp::start(app).await?;
    ///
    /// tb.broker::<MemoryBroker>()
    ///     .raw(b"frame")
    ///     .to("frames")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn raw<'p, B>(
        &self,
        payload: &'p B,
    ) -> Publish<InjectSink<'a>, RawBody<'p>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(self.sink(), payload)
    }

    /// This handle's transport as a publish sink. It borrows the app, not the handle, so a
    /// builder started on a temporary handle outlives it.
    fn sink(&self) -> InjectSink<'a> {
        InjectSink(Target::Broker {
            coordinator: self.coordinator,
            testable: self.testable,
            token: self.token,
            label: self.label.clone(),
        })
    }
}

impl BrokerHandle<'_> {
    /// Publishes `value` (encoded with [`DefaultCodec`](crate::codec::DefaultCodec)) to `name`, then
    /// drives the resulting reaction to a standstill before returning.
    ///
    /// The builder did not replace this one, which is why it outlived the byte-publishing method
    /// beside it. [`message`](Self::message) reads the destination form off the value's type
    /// through [`OutgoingDestination`], which `#[derive(Outgoing)]` implements - so a `Serialize`
    /// type the test does not own is out of reach: the orphan rule forbids both the derive and a
    /// hand-written impl on a foreign type. Derive `Outgoing` on the injected type and inject it
    /// through the builder wherever that is possible; this method stays for the case where it is
    /// not.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::ShutDown`] if the service has been torn down, [`TestError::Encode`]
    /// if the value does not encode, [`TestError::NoTransport`] if this broker has no
    /// in-process test transport, or [`TestError::NotQuiescent`] if the reaction does not
    /// settle.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub async fn publish<T: serde::Serialize + Sync>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), TestError> {
        let bytes = DefaultCodec::default()
            .encode(value)
            .map_err(|err| TestError::Encode(err.to_string()))?;
        self.sink().send(OutgoingMessage::new(name, &bytes)).await
    }

    /// Like [`publish`](Self::publish), but with headers on the delivery: `headers` is a typed
    /// contract serialized into the header map (see
    /// [`Headers::insert_typed`](crate::Headers::insert_typed)) - the input a
    /// [`FromHeaders`](crate::runtime::FromHeaders) handler parses.
    ///
    /// Kept for the reason spelled out on [`publish`](Self::publish): the builder's
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
        self.sink().send(msg).await
    }

    /// Asserts on what the handler subscribed to `name` received and how it settled.
    #[must_use]
    pub fn subscriber(&self, name: &str) -> SubscriberAssertions<'_> {
        SubscriberAssertions::new(self.coordinator, self.scope_id, name.to_owned())
    }

    /// Asserts on what was published to `name` on this broker (the broker's publish log).
    #[must_use]
    pub fn published<T>(&self, name: &str) -> PublishedAssertions<T> {
        let messages = self.testable.map(|t| t.published(name)).unwrap_or_default();
        PublishedAssertions::new(name.to_owned(), messages)
    }
}
