//! The [`TestApp`] harness: drives a built [`RustStream`](crate::runtime::RustStream) application in
//! process, with no network `connect` and no server, and exposes per-broker assertions.

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::OutgoingMessage;
use crate::runtime::{
    BrokerLifecycle, ErrorShutdown, LifecycleHook, PublishIdentity, RegisteredBroker, RustStream,
    RustStreamError, Starter, TestParts,
};

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

/// One broker registered in the app under test: its label, its erased lifecycle handle (for
/// type/label addressing), and the registration that recovers its [`TestableBroker`] view (when it
/// is registered with [`register_testable_broker!`](crate::register_testable_broker)).
struct BrokerEntry {
    label: Option<String>,
    lifecycle: Arc<dyn BrokerLifecycle>,
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
    lifecycle: &Arc<dyn BrokerLifecycle>,
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

impl std::fmt::Debug for TestBrokers<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokers")
            .field("brokers", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl TestBrokers<'_> {
    /// Returns the unique registered broker of type `B`, for building a mirror state's publishers
    /// (`tb.broker::<MemoryBroker>().publisher()`).
    ///
    /// # Panics
    ///
    /// Panics if no broker of type `B` is registered, or more than one is (disambiguate the app, or
    /// address by label is not supported when building state).
    #[must_use]
    pub fn broker<B: crate::Broker + 'static>(&self) -> &B {
        let mut found = self
            .entries
            .iter()
            .filter_map(|e| e.lifecycle.as_any().downcast_ref::<B>());
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

/// In-process harness around a built application: drives input through the broker bus (no `connect`,
/// no server), records what handlers saw and published, and exposes per-broker assertions.
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
pub struct TestApp<St> {
    entries: Vec<BrokerEntry>,
    coordinator: Coordinator,
    #[allow(dead_code)]
    state: Arc<St>,
    error_shutdown: ErrorShutdown,
    token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
}

impl<St> std::fmt::Debug for TestApp<St> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestApp")
            .field("brokers", &self.entries.len())
            .field("subscribers", &self.handles.len())
            .finish_non_exhaustive()
    }
}

impl<St: Send + Sync + 'static> TestApp<St> {
    /// Starts the harness by running the app's real `on_startup` (the existing state and its
    /// publishers bind to the in-process bus). No broker `connect` runs.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Startup`] if a lifecycle hook fails, or [`TestError::Subscribe`] if a
    /// subscription fails to open.
    pub async fn start<L, Phase>(
        app: RustStream<L, St, PublishIdentity, Phase>,
    ) -> Result<Self, TestError> {
        let (coordinator, entries, parts) = Self::setup(app);
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
    /// dependencies. No broker `connect` runs.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Subscribe`] if a subscription fails to open.
    pub async fn with_state<L, F, Phase>(
        app: RustStream<L, St, PublishIdentity, Phase>,
        build: F,
    ) -> Result<Self, TestError>
    where
        F: FnOnce(&TestBrokers<'_>) -> St,
    {
        let (coordinator, entries, parts) = Self::setup(app);
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

    /// Installs a fresh coordinator into the app's hooks slot and each broker's bus, and recovers
    /// the per-broker transports. Returns the coordinator, the broker entries, and the remaining
    /// parts (the brokers field is now consumed and empty).
    fn setup<L, Phase>(
        app: RustStream<L, St, PublishIdentity, Phase>,
    ) -> (Coordinator, Vec<BrokerEntry>, TestParts<St>) {
        let mut parts = app.into_test_parts();
        let coordinator = Coordinator::new(DEFAULT_MAX_STEPS);
        parts.test_hooks.install(coordinator.clone());
        let entries = std::mem::take(&mut parts.brokers)
            .into_iter()
            .map(|RegisteredBroker { lifecycle, label }| {
                let registration = recover_testable(&lifecycle, &coordinator);
                BrokerEntry {
                    label,
                    lifecycle,
                    registration,
                }
            })
            .collect();
        (coordinator, entries, parts)
    }

    /// Spawns the dispatch loops against the (uninstalled) bus and runs `after_startup`, completing
    /// the harness. No broker `connect` runs.
    async fn spawn(args: SpawnArgs<St>) -> Result<Self, TestError> {
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
        let mut matches = self
            .entries
            .iter()
            .filter(|e| e.lifecycle.as_any().downcast_ref::<B>().is_some());
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
struct SpawnArgs<St> {
    coordinator: Coordinator,
    entries: Vec<BrokerEntry>,
    starters: Vec<Starter<St>>,
    after_startup: Vec<LifecycleHook<St>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
    state: Arc<St>,
}

/// A handle to one broker in a [`TestApp`]: inject input and assert on its handlers and publishes.
pub struct BrokerHandle<'a> {
    scope_id: usize,
    coordinator: &'a Coordinator,
    testable: Option<&'a dyn TestableBroker>,
    token: &'a CancellationToken,
    label: String,
}

impl std::fmt::Debug for BrokerHandle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerHandle")
            .field("broker", &self.label)
            .field("testable", &self.testable.is_some())
            .finish_non_exhaustive()
    }
}

impl BrokerHandle<'_> {
    /// Publishes `value` (encoded with [`DefaultCodec`](crate::codec::DefaultCodec)) to `name`, then
    /// drives the resulting reaction to a standstill before returning.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::ShutDown`] if the service has been torn down, [`TestError::Encode`] if
    /// the value does not encode, or [`TestError::NotQuiescent`] if the reaction does not settle.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub async fn publish<T: serde::Serialize + Sync>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), TestError> {
        use crate::codec::Codec;
        let bytes = crate::codec::DefaultCodec::default()
            .encode(value)
            .map_err(|err| TestError::Encode(err.to_string()))?;
        self.publish_raw(name, &bytes).await
    }

    /// Publishes raw `payload` bytes to `name`, then drives the reaction to a standstill.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::ShutDown`] if the service has been torn down, [`TestError::NoTransport`]
    /// if this broker has no in-process transport, or [`TestError::NotQuiescent`] if the reaction
    /// does not settle.
    pub async fn publish_raw(&self, name: &str, payload: &[u8]) -> Result<(), TestError> {
        if self.token.is_cancelled() {
            return Err(TestError::ShutDown);
        }
        let transport = self
            .testable
            .ok_or_else(|| TestError::NoTransport(self.label.clone()))?;
        transport.inject(OutgoingMessage::new(name, payload));
        self.coordinator.drive().await
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
