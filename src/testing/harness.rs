//! The [`TestApp`] harness: drives a built [`RustStream`](crate::runtime::RustStream) application
//! and exposes per-broker assertions. In process, each broker of the app connects through its
//! registered [`InProcess`] transition, which produces the production connected form with no I/O;
//! live, each connects through its ordinary `connect`, against a running stand.

use std::any::{TypeId, type_name};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant as WallClock};

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use crate::OutgoingDestination;
use crate::runtime::{
    App, ConnectedLifecycle, HeadersUnset, LifecycleHook, OutSlot, PublishBuilder,
    RegisteredBroker, RustStreamError, Shutdown, Starter, TestParts,
};
use crate::runtime::{MessageBody, UnnamedCodec, message_of};
use crate::{Broker, DefaultPublish, PublishPolicy, Publisher};

use super::assertions::PublishedAssertions;
use super::broker::{LivePublish, TestableBroker, TestableRegistration, live_publish};
use super::coordinator::{Coordinator, LiveSubscription};
use super::handle::{BrokerHandle, Harness, InjectSink, Target, Transport};

/// The default cap on dispatched deliveries before [`TestApp::publish`] gives up driving a reaction
/// to quiescence. Guards against a non-terminating requeue loop.
const DEFAULT_MAX_STEPS: usize = 10_000;

/// How long a live settle waits by default for the reaction to settle before it reports the
/// subscription that did not.
const DEFAULT_SETTLE_DEADLINE: Duration = Duration::from_secs(10);

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
    /// A live reaction did not settle before the deadline: a subscription had not handled what was
    /// published to a destination it reads, or a redelivery that fell due, or a handler was still
    /// running.
    #[error(
        "the live reaction did not settle within {deadline:?}: {}",
        unsettled(subscription.as_deref(), *handled, *expected)
    )]
    NotSettled {
        /// The subscription still owed a delivery, or `None` when every subscription had handled
        /// what it was owed and a handler was still running.
        subscription: Option<String>,
        /// How many deliveries that subscription had handled.
        handled: usize,
        /// How many it was owed: the publishes the harness saw to it, and the redeliveries due.
        expected: usize,
        /// The deadline the harness was started with.
        deadline: Duration,
    },
    /// A broker failed to connect while starting the harness: its in-process transition, or its
    /// `connect` in live mode.
    #[error("broker {broker} failed to connect: {source}")]
    Connect {
        /// The broker's label, or its type for unlabeled brokers.
        broker: String,
        /// The broker's own connect error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A live broker refused a message the test published onto it.
    #[error("broker {broker} refused the test's publish: {source}")]
    Publish {
        /// The broker's label, or its type for unlabeled brokers.
        broker: String,
        /// The broker's own publish error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A publish was attempted after a fail-fast failure tore the service down.
    #[error("publish after the service shut down")]
    ShutDown,
    /// An unscoped [`TestApp::publish`] is ambiguous: more than one broker is registered.
    #[error("more than one broker is registered; address one with broker::<B>() or broker_named()")]
    Ambiguous,
    /// A broker of the app has no in-process mode, so the harness cannot start the app: its type
    /// is not registered with [`register_testable_broker!`](crate::register_testable_broker),
    /// which is what a broker crate does under its `testing` feature.
    ///
    /// In live mode it reports an injection through an untyped handle (the sole broker,
    /// [`broker_named`](TestApp::broker_named)) onto a broker of such a type: address it with
    /// [`broker`](TestApp::broker) instead, which publishes through the type's default policy.
    #[error(
        "broker {0} has no in-process mode; enable the `testing` feature of the crate that \
         provides it in `[dev-dependencies]`"
    )]
    NoTransport(String),
    /// [`TestApp::start_live`] was called on a paused clock. A network client's timeouts would
    /// fire at once against it, so a live test runs on the real clock.
    #[error(
        "the clock is paused, and a live test runs against brokers on the real clock; drop \
         `start_paused` / `tokio::time::pause()` from this test, or start it in process with \
         `TestApp::start`"
    )]
    PausedClock,
    /// The message failed to encode for publishing.
    #[error("failed to encode the message: {0}")]
    Encode(String),
}

/// What a live settle was still waiting on at its deadline, for [`TestError::NotSettled`].
fn unsettled(subscription: Option<&str>, handled: usize, expected: usize) -> String {
    subscription.map_or_else(
        || "a handler was still running".to_owned(),
        |name| format!("subscription {name:?} handled {handled} of {expected} deliveries"),
    )
}

/// One broker of the app under test: its label, its erased connected handle (for type/label
/// addressing), and its type's registration, when a broker crate made one. In process every broker
/// has one, since it connected through it.
pub(super) struct BrokerEntry {
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

/// How a broker the harness has no registration for is taken to route: to every subscription of
/// the destination's name.
fn same_names(destination: &str, subscriptions: &[&str]) -> Vec<usize> {
    subscriptions
        .iter()
        .enumerate()
        .filter(|(_, name)| **name == destination)
        .map(|(position, _)| position)
        .collect()
}

/// The registration of the broker type `broker`, or `None` when no crate registered one with
/// [`register_testable_broker!`](crate::register_testable_broker).
fn registration_of(broker: TypeId) -> Option<&'static TestableRegistration> {
    inventory::iter::<TestableRegistration>
        .into_iter()
        .find(|registration| registration.registers(broker))
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
    pub fn broker<B: Broker + 'static>(&self) -> &B::Connected {
        let mut found = self
            .entries
            .iter()
            .filter(|e| e.lifecycle.broker_type() == TypeId::of::<B>())
            .filter_map(|e| e.lifecycle.as_any().downcast_ref::<B::Connected>());
        let first = found
            .next()
            .unwrap_or_else(|| panic!("no registered broker of type {}", type_name::<B>()));
        assert!(
            found.next().is_none(),
            "more than one broker of type {} is registered",
            type_name::<B>(),
        );
        first
    }
}

/// A test harness around the service's production app.
///
/// Takes the app `main` builds, unchanged. [`start`](Self::start) connects each of its brokers
/// through the broker's [`InProcess`](super::InProcess) transition instead of `connect`, so the
/// same connected forms carry the routes with no server; [`start_live`](Self::start_live)
/// connects them through `connect`, against a running stand. Either way the harness drives input
/// through the broker, records what handlers saw and what the app published, and exposes
/// per-broker assertions addressed by the production broker type, so one test body runs in both
/// modes and only the start call differs.
///
/// Build one with [`start`](Self::start) (runs the app's real `on_startup`) or
/// [`with_state`](Self::with_state) (injects a mirror state for non-broker dependencies). Drive
/// input with [`broker`](Self::broker) / [`broker_named`](Self::broker_named) and assert on what
/// happened.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "testing", feature = "memory", feature = "macros", feature = "json"))]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
/// use ruststream::testing::TestApp;
/// use ruststream::{Outgoing, subscriber};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Outgoing, Serialize, Deserialize, PartialEq, Debug)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn handle(order: &Order) -> HandlerOutcome {
///     let _ = order;
///     HandlerOutcome::ack()
/// }
///
/// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
///     .with_broker(MemoryBroker::new(), |b| { b.include(handle); });
/// let tb = TestApp::start(app).await?;
///
/// tb.broker::<MemoryBroker>()
///     .message(&Order { id: 1 })
///     .to("orders")
///     .publish()
///     .await?;
/// tb.broker::<MemoryBroker>()
///     .subscriber("orders")
///     .assert_called_once()
///     .with(&Order { id: 1 })
///     .settled(HandlerOutcome::ack());
/// # Ok(())
/// # }
/// ```
pub struct TestApp<State> {
    entries: Vec<BrokerEntry>,
    mode: Mode,
    coordinator: Coordinator,
    #[allow(dead_code)]
    state: Arc<State>,
    shutdown: Shutdown,
    handles: Vec<JoinHandle<()>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
}

impl<State> fmt::Debug for TestApp<State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestApp")
            .field("mode", &self.mode)
            .field("brokers", &self.entries.len())
            .field("subscribers", &self.handles.len())
            .finish_non_exhaustive()
    }
}

/// Which transport the harness connected the app's brokers to.
///
/// A run-time value rather than a type parameter: every method of the harness has a meaning in
/// both modes, so nothing is closed off by it.
#[derive(Debug)]
pub(super) enum Mode {
    /// Each broker through its in-process transition: the transports count what is in flight, so
    /// a settle waits for that count to reach zero.
    InProcess,
    /// Each broker through its `connect`, against a running stand.
    Live {
        /// How long a settle waits for the reaction.
        deadline: Duration,
        /// The app's subscriptions, which a settle waits on.
        subscriptions: Vec<LiveSubscription>,
    },
}

impl Mode {
    /// Waits for the reaction the harness can see to settle: the in-process transports' count
    /// reaching zero, or, live, every subscription having handled what it is owed.
    pub(super) async fn settle(
        &self,
        coordinator: &Coordinator,
        brokers: &[BrokerEntry],
    ) -> Result<(), TestError> {
        match self {
            Self::InProcess => coordinator.drive().await,
            Self::Live {
                deadline,
                subscriptions,
            } => {
                // Each broker routes its own publishes: the harness asks it, and knows no routing
                // but equal names for a broker it has no registration for.
                let routing = |scope_id: usize, destination: &str, names: &[&str]| {
                    brokers[scope_id].testable().map_or_else(
                        || same_names(destination, names),
                        |broker| broker.routes(destination, names),
                    )
                };
                coordinator
                    .settle_live(subscriptions, &routing, *deadline)
                    .await
            }
        }
    }
}

/// Whether the clock of the current runtime is paused.
///
/// Why this is measured rather than asked: tokio exposes no query for it. A paused clock stands
/// still until a timer is awaited, while a running one moves with the wall clock, so the two
/// readings below tell them apart.
fn clock_is_paused() -> bool {
    let before = tokio::time::Instant::now();
    let wall = WallClock::now();
    while wall.elapsed() < Duration::from_micros(50) {
        std::hint::spin_loop();
    }
    tokio::time::Instant::now() == before
}

impl<State: Send + Sync + 'static> TestApp<State> {
    /// Starts the harness in the service's own startup order: the app's real `on_startup`, then
    /// each broker's connect, then the subscriptions, then `after_startup`. Each broker connects
    /// through its [`InProcess`](super::InProcess) transition, which performs no I/O; the rest of
    /// startup is the production one, so an `on_startup` that fails leaves no broker connected.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NoTransport`] if a broker of the app has no in-process mode,
    /// [`TestError::Startup`] if a lifecycle hook fails, [`TestError::Connect`] if a broker fails
    /// to connect in process, or [`TestError::Subscribe`] if a subscription fails to open.
    /// The app's publish pipeline is whatever it was built with: an app-wide
    /// [`publish_layer`](crate::runtime::RustStream::publish_layer) is already baked into the mounted routes, so
    /// the harness drives the same stack production would.
    pub async fn start<A>(app: A) -> Result<Self, TestError>
    where
        A: App<State = State>,
    {
        let (coordinator, parts) = Self::prepare(app);
        let TestParts {
            brokers,
            starters,
            state_init,
            after_startup,
            shutdown_timeout,
            continuations,
            ..
        } = parts;
        let registered = registrations(brokers)?;
        // The service produces its state before any broker connects, so a failing `on_startup`
        // leaves nothing connected, and the producer never sees a connected broker.
        let state = state_init().await.map_err(TestError::Startup)?;
        let entries = connect_in_process(registered, &coordinator).await?;
        Self::spawn(SpawnArgs {
            coordinator,
            entries,
            mode: Mode::InProcess,
            starters,
            after_startup,
            continuations,
            shutdown_timeout,
            state: Arc::new(state),
        })
        .await
    }

    /// Starts the harness against running brokers: each broker of the app connects through its
    /// ordinary `connect`, and the rest of startup is the production one, in its order: the app's
    /// real `on_startup` runs before any broker connects.
    ///
    /// The test body is the one an in-process test runs; only this call differs. What differs
    /// underneath is how long things take. A publish, [`settle`](Self::settle) and
    /// [`advance`](Self::advance) wait on the broker's real clock, up to a deadline of ten
    /// seconds ([`start_live_within`](Self::start_live_within) sets another), for the
    /// subscriptions to handle what was published to them and what fell due.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::PausedClock`] on a paused clock, [`TestError::Startup`] if a lifecycle
    /// hook fails, [`TestError::Connect`] if a broker fails to connect, or
    /// [`TestError::Subscribe`] if a subscription fails to open.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "testing", feature = "memory", feature = "macros", feature = "json"))]
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
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn accept(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(accept);
    ///     })
    /// }
    ///
    /// let tb = TestApp::start_live(app()).await?;
    /// tb.broker::<MemoryBroker>().message(&Order { id: 1 }).publish().await?;
    /// tb.broker::<MemoryBroker>().subscriber("orders").assert_called_once();
    /// tb.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_live<A>(app: A) -> Result<Self, TestError>
    where
        A: App<State = State>,
    {
        Self::start_live_within(app, DEFAULT_SETTLE_DEADLINE).await
    }

    /// [`start_live`](Self::start_live) with `deadline` as the longest a publish, a settle or an
    /// advance waits for the reaction to settle.
    ///
    /// # Errors
    ///
    /// As [`start_live`](Self::start_live).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "testing", feature = "memory"))]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use std::time::Duration;
    ///
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, RustStream};
    /// use ruststream::testing::TestApp;
    ///
    /// // A stand far away: give the reaction half a minute before the test gives up on it.
    /// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///     .register_broker(MemoryBroker::new());
    /// let tb = TestApp::start_live_within(app, Duration::from_secs(30)).await?;
    /// tb.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_live_within<A>(app: A, deadline: Duration) -> Result<Self, TestError>
    where
        A: App<State = State>,
    {
        if clock_is_paused() {
            return Err(TestError::PausedClock);
        }
        let (coordinator, parts) = Self::prepare(app);
        let TestParts {
            brokers,
            starters,
            state_init,
            after_startup,
            shutdown_timeout,
            continuations,
            test_hooks,
        } = parts;
        // As in process: the state first, the brokers after it.
        let state = state_init().await.map_err(TestError::Startup)?;
        let entries = connect_live(brokers).await?;
        let subscriptions = test_hooks.subscriptions();
        let mode = Mode::Live {
            deadline,
            subscriptions,
        };
        Self::spawn(SpawnArgs {
            coordinator,
            entries,
            mode,
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
    /// the same transports (`tb.broker::<MemoryBroker>().publisher()`) and supply fakes for
    /// non-broker dependencies. Each broker connects in process first, so the mirror state's
    /// publishers pair against connected brokers.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NoTransport`] if a broker of the app has no in-process mode,
    /// [`TestError::Connect`] if a broker fails to connect in process, or
    /// [`TestError::Subscribe`] if a subscription fails to open.
    pub async fn with_state<A, F>(app: A, build: F) -> Result<Self, TestError>
    where
        A: App<State = State>,
        F: FnOnce(&TestBrokers<'_>) -> State,
    {
        let (coordinator, parts) = Self::prepare(app);
        let TestParts {
            brokers,
            starters,
            after_startup,
            shutdown_timeout,
            continuations,
            ..
        } = parts;
        let registered = registrations(brokers)?;
        let entries = connect_in_process(registered, &coordinator).await?;
        let state = build(&TestBrokers { entries: &entries });
        Self::spawn(SpawnArgs {
            coordinator,
            entries,
            mode: Mode::InProcess,
            starters,
            after_startup,
            continuations,
            shutdown_timeout,
            state: Arc::new(state),
        })
        .await
    }

    /// Takes the app apart and installs a fresh coordinator into its hooks slot, before anything
    /// of it runs.
    fn prepare<A>(app: A) -> (Coordinator, TestParts<State>)
    where
        A: App<State = State>,
    {
        let parts = app.into_test_parts();
        let coordinator = Coordinator::new(DEFAULT_MAX_STEPS);
        parts.test_hooks.install(coordinator.clone());
        (coordinator, parts)
    }

    /// Spawns the dispatch loops against the connected brokers and runs `after_startup`,
    /// completing the harness.
    async fn spawn(args: SpawnArgs<State>) -> Result<Self, TestError> {
        let SpawnArgs {
            coordinator,
            entries,
            mode,
            starters,
            after_startup,
            continuations,
            shutdown_timeout,
            state,
        } = args;
        // A publisher the runtime pairs names the connected broker it was paired against; this
        // is how the harness knows which registration that is.
        coordinator.locate(entries.iter().map(|entry| entry.lifecycle.as_any()));
        let shutdown = Shutdown::new();
        let mut handles = Vec::with_capacity(starters.len());
        let failed = 'start: {
            for starter in starters {
                match starter(state.clone(), shutdown.clone()).await {
                    Ok(handle) => handles.push(handle),
                    Err(err) => break 'start Some(TestError::Subscribe(err)),
                }
            }
            for hook in after_startup {
                if let Err(err) = hook(state.clone()).await {
                    break 'start Some(TestError::Startup(err));
                }
            }
            None
        };
        if let Some(err) = failed {
            // Unwound the way the service unwinds a failed start: what is running stops, and the
            // brokers already connected are shut down rather than dropped.
            stop_dispatch(&shutdown, handles, shutdown_timeout).await;
            drain_continuations(&continuations, shutdown_timeout).await;
            shut_down(entries).await;
            return Err(err);
        }
        Ok(Self {
            entries,
            mode,
            coordinator,
            state,
            shutdown,
            handles,
            continuations,
            shutdown_timeout,
        })
    }

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
        self.handle(entry, None)
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
    /// transaction's buffer) are not attributed to the slot.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
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
        PublishedAssertions::captured(
            format!("Out slot `{}`", M::NAME),
            self.coordinator.slot_published(M::NAME),
        )
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
            .position(|e| std::ptr::eq(e, entry))
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
            Mode::InProcess => tokio::time::advance(by).await,
            // Why real time passes here: the delay belongs to the real broker's own timer, which a
            // paused clock in this process does not reach.
            Mode::Live { .. } => tokio::time::sleep(by).await,
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

    /// Shuts the harness down: stops the dispatch loops, drains in-flight handlers and post-settle
    /// continuations (bounded by the app's shutdown timeout), shuts each broker down, and returns
    /// [`run_result`](Self::run_result).
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError::Dispatch`] when a fail-fast failure tore the service down.
    pub async fn shutdown(self) -> Result<(), RustStreamError> {
        stop_dispatch(&self.shutdown, self.handles, self.shutdown_timeout).await;
        self.continuations.close();
        self.continuations.wait().await;
        shut_down(self.entries).await;
        self.shutdown
            .taken_failure()
            .map_or(Ok(()), |reason| Err(RustStreamError::Dispatch(reason)))
    }
}

/// Stops the dispatch loops: signals them, and waits for them all, bounded by one shutdown
/// timeout; the loops still running at its end are aborted with their workers, as the service's
/// own shutdown does.
async fn stop_dispatch(
    shutdown: &Shutdown,
    handles: Vec<JoinHandle<()>>,
    timeout: Option<Duration>,
) {
    shutdown.cancel();
    let until = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    for mut handle in handles {
        let Some(until) = until else {
            let _ = handle.await;
            continue;
        };
        if tokio::time::timeout_at(until, &mut handle).await.is_err() {
            handle.abort();
            // Awaited, so the loop is gone, not merely told to go, when this returns.
            let _ = handle.await;
        }
    }
}

/// Closes the post-settle continuations and waits for them, bounded by the app's shutdown timeout.
async fn drain_continuations(continuations: &TaskTracker, timeout: Option<Duration>) {
    continuations.close();
    match timeout {
        Some(timeout) => {
            let _ = tokio::time::timeout(timeout, continuations.wait()).await;
        }
        None => continuations.wait().await,
    }
}

/// Shuts the connected brokers down in reverse connect order, as the service does. A live broker
/// holds a connection, which is closed the way the service closes it; a teardown failure there
/// says nothing about the service under test.
async fn shut_down(entries: Vec<BrokerEntry>) {
    for entry in entries.into_iter().rev() {
        let broker = entry.display();
        if let Err(err) = entry.lifecycle.shutdown().await {
            tracing::debug!(
                target: "ruststream::testing",
                broker = %broker,
                error = %err,
                "broker shutdown after the test failed",
            );
        }
    }
}

/// Looks up the registration of every broker of the app, before any of them connects: an app with
/// one unregistered broker fails without having half-started the others.
fn registrations(
    brokers: Vec<RegisteredBroker>,
) -> Result<Vec<(RegisteredBroker, &'static TestableRegistration)>, TestError> {
    brokers
        .into_iter()
        .map(|broker| {
            // Why this is a startup error rather than a compile error: `RustStream` erases the
            // types of its brokers when they are registered, so `TestApp::start` receives an app
            // whose broker types it cannot name in a bound.
            let registration = registration_of(broker.lifecycle.broker_type())
                .ok_or_else(|| TestError::NoTransport(broker.lifecycle.broker_name().to_owned()))?;
            Ok((broker, registration))
        })
        .collect()
}

/// Connects each broker through its registered in-process transition, in registration order, and
/// installs the coordinator into each transport. A broker that fails to connect shuts down the
/// ones connected before it.
async fn connect_in_process(
    registered: Vec<(RegisteredBroker, &'static TestableRegistration)>,
    coordinator: &Coordinator,
) -> Result<Vec<BrokerEntry>, TestError> {
    let mut entries = Vec::with_capacity(registered.len());
    for (RegisteredBroker { lifecycle, label }, registration) in registered {
        let broker = label
            .clone()
            .unwrap_or_else(|| lifecycle.broker_name().to_owned());
        let lifecycle = match lifecycle
            .connect_in_process(registration.transition())
            .await
        {
            Ok(lifecycle) => lifecycle,
            Err(source) => {
                shut_down(entries).await;
                return Err(TestError::Connect { broker, source });
            }
        };
        let entry = BrokerEntry {
            label,
            lifecycle,
            registration: Some(registration),
        };
        entry
            .testable()
            .expect("a registration resolves the connected form its own transition produced")
            .install_coordinator(coordinator.clone());
        entries.push(entry);
    }
    Ok(entries)
}

/// Connects each broker through its ordinary `connect`, in registration order. A broker that
/// fails to connect shuts down the ones connected before it.
async fn connect_live(brokers: Vec<RegisteredBroker>) -> Result<Vec<BrokerEntry>, TestError> {
    let mut entries = Vec::with_capacity(brokers.len());
    for RegisteredBroker { lifecycle, label } in brokers {
        let broker = label
            .clone()
            .unwrap_or_else(|| lifecycle.broker_name().to_owned());
        // The registration is not needed to run live; an untyped handle publishes through it.
        let registration = registration_of(lifecycle.broker_type());
        let lifecycle = match lifecycle.connect().await {
            Ok(lifecycle) => lifecycle,
            Err(source) => {
                shut_down(entries).await;
                return Err(TestError::Connect { broker, source });
            }
        };
        entries.push(BrokerEntry {
            label,
            lifecycle,
            registration,
        });
    }
    Ok(entries)
}

/// The pieces [`TestApp::spawn`] needs to start the dispatch loops.
struct SpawnArgs<State> {
    coordinator: Coordinator,
    entries: Vec<BrokerEntry>,
    mode: Mode,
    starters: Vec<Starter<State>>,
    after_startup: Vec<LifecycleHook<State>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
    state: Arc<State>,
}
