//! The [`TestApp`] harness: drives a built [`RustStream`](ruststream_runtime_tokio::RustStream) application
//! and exposes per-broker assertions. In process, each broker of the app connects through its
//! registered [`InProcess`] transition, which produces the production connected form with no I/O;
//! live, each connects through its ordinary `connect`, against a running stand.

use std::fmt;
use std::hint;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant as WallClock};

use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Instant;
use tokio_util::task::TaskTracker;

use ruststream_runtime_tokio::{App, RustStreamError, SubscriptionEnd};
use ruststream_runtime_tokio::{
    ConnectedEntry, LifecycleHook, RegisteredBroker, Shutdown, ShutdownHooks, Starter, Teardown,
    TestParts,
};

use ruststream_runtime::testing::Coordinator;
use ruststream_runtime::testing::{TestError, TestableRegistration};

use ruststream_runtime::testing::HookFailures;

mod brokers;
mod drive;

pub use brokers::TestBrokers;
use brokers::registration_of;
pub(crate) use brokers::{BrokerEntry, Mode};

/// The default cap on dispatched deliveries before [`TestApp::publish`] gives up driving a reaction
/// to quiescence. Guards against a non-terminating requeue loop.
const DEFAULT_MAX_STEPS: usize = 10_000;

/// How long a live settle waits by default for the reaction to settle before it reports the
/// subscription that did not.
const DEFAULT_SETTLE_DEADLINE: Duration = Duration::from_secs(10);

/// A test harness around the service's production app.
///
/// Takes the app `main` builds, unchanged. [`start`](Self::start) connects each of its brokers
/// through the broker's [`InProcess`](ruststream_runtime::testing::InProcess) transition instead of `connect`, so the
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
/// Every subscription loop and every worker of `workers(n)` runs as a task on the
/// test's own runtime, whatever runtime the generated `main` builds, and a `threads(n)`
/// subscription runs as `n` such workers rather than on threads of its own, so
/// [`advance`](Self::advance) reaches every timer the app arms and [`settle`](Self::settle)
/// sees every delivery.
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
///     .with_broker(MemoryBroker::new(), |b| { b.include(handle) });
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
    // The dispatch tasks own the app's state; the marker gives this type the auto traits of the
    // `Arc<State>` they share.
    _state: PhantomData<Arc<State>>,
    shutdown: Shutdown,
    handles: Vec<JoinHandle<()>>,
    // Why a lock nothing ever takes: the bound hooks are `Send` but not `Sync`, and the mutex
    // keeps the harness `Sync` (tests share `&TestApp` across awaits); `shutdown` moves them out.
    hooks: Mutex<ShutdownHooks>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
    hook_failures: HookFailures,
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

/// Whether the clock of the current runtime is paused.
///
/// Why this is measured rather than asked: tokio exposes no query for it. A paused clock stands
/// still until a timer is awaited, while a running one moves with the wall clock, so the two
/// readings below tell them apart.
fn clock_is_paused() -> bool {
    let before = Instant::now();
    let wall = WallClock::now();
    while wall.elapsed() < Duration::from_micros(50) {
        hint::spin_loop();
    }
    Instant::now() == before
}

impl<State: Send + Sync + 'static> TestApp<State> {
    /// Starts the harness in the service's own startup order: the app's real `on_startup`, then
    /// each broker's connect, then the subscriptions, then `after_startup`. Each broker connects
    /// through its [`InProcess`](ruststream_runtime::testing::InProcess) transition, which performs no I/O; the rest of
    /// startup is the production one, so an `on_startup` that fails leaves no broker connected.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NoTransport`] if a broker of the app has no in-process mode,
    /// [`TestError::Startup`] if a lifecycle hook fails, [`TestError::Connect`] if a broker fails
    /// to connect in process, or [`TestError::Subscribe`] if a subscription fails to open.
    ///
    /// The app's publish pipeline is whatever it was built with: an app-wide
    /// [`publish_layer`](ruststream_runtime_tokio::RustStream::publish_layer) is already baked into the mounted routes, so
    /// the harness drives the same stack production would.
    ///
    /// # Examples
    ///
    /// The publish drives the whole reaction to completion before it returns: the handler, its
    /// downstream publishes, any cross-broker cascade. Then the test asserts.
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::testing::TestApp;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
    /// struct Order {
    ///     id: u64,
    ///     quantity: u32,
    /// }
    ///
    /// #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
    /// #[outgoing(name = "confirmations")]
    /// struct Confirmation {
    ///     id: u64,
    ///     accepted: bool,
    /// }
    ///
    /// #[subscriber("orders", reply)]
    /// async fn confirm(order: &Order) -> Confirmation {
    ///     Confirmation {
    ///         id: order.id,
    ///         accepted: order.quantity > 0,
    ///     }
    /// }
    ///
    /// /// The app `main` runs, and the one the tests hand the harness.
    /// pub fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(confirm).out_reply(Publish)
    ///     })
    /// }
    ///
    /// pub async fn confirms_valid_orders() -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::start(app()).await?;
    ///     tb.broker::<MemoryBroker>()
    ///         .message(&Order { id: 1, quantity: 2 })
    ///         .to("orders")
    ///         .publish()
    ///         .await?;
    ///
    ///     tb.broker::<MemoryBroker>()
    ///         .subscriber("orders")
    ///         .assert_called_once()
    ///         .with(&Order { id: 1, quantity: 2 })
    ///         .settled(HandlerOutcome::ack());
    ///     tb.broker::<MemoryBroker>()
    ///         .published::<Confirmation>("confirmations")
    ///         .assert_called_once()
    ///         .with(&Confirmation {
    ///             id: 1,
    ///             accepted: true,
    ///         });
    ///     Ok(())
    /// }
    /// # }
    /// # #[cfg(feature = "json")]
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     tokio::runtime::Builder::new_multi_thread()
    /// #         .enable_all()
    /// #         .build()?
    /// #         .block_on(demo::confirms_valid_orders())
    /// # }
    /// # #[cfg(not(feature = "json"))]
    /// # fn main() {}
    /// ```
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
            on_shutdown,
            after_shutdown,
            shutdown_timeout,
            subscription_end,
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
            on_shutdown,
            after_shutdown,
            continuations,
            shutdown_timeout,
            subscription_end,
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
    /// # What a live test sees
    ///
    /// What changes live is how the harness knows the reaction is over. It cannot read a broker's
    /// queues, so it records what is published through the app's own wiring: the test's publishes,
    /// and every message a publisher the runtime paired for the app's wiring hands a broker. That
    /// covers replies, `Out` slot publishes and requests, retry copies, dead letters, and a `Bound`
    /// token's publishes through any of them. Each is recorded on the broker it goes to: the one
    /// the test named, or the one the publisher was paired against. A handler on one broker holding
    /// a `Bound` token for another publishes to the other, and the publish is that broker's.
    ///
    /// A publish, a settle and an advance then wait until every subscription has handled each
    /// recorded publish its broker delivers to it, and every redelivery that fell due, and no
    /// handler is running. Which subscriptions a publish reaches is the broker's own routing, which
    /// the broker answers through [`TestableBroker::routes`](ruststream_runtime::testing::TestableBroker::routes): an
    /// equal name for [`MemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.MemoryBroker.html), every matching subscription
    /// for a broker that fans out, the one it picks for a broker that picks one. A subscription of
    /// another broker never owes it. Each subscription counts what it handled itself, so two
    /// subscriptions reporting one name (two subscriptions on one topic) each owe the publish. The
    /// wait is bounded: ten seconds by default, [`start_live_within`](Self::start_live_within) sets
    /// another, and past it the call fails with [`TestError::NotSettled`] naming the subscription
    /// it was waiting on. A live [`advance`](Self::advance) lets the time pass for real, because
    /// the delay is the broker's own timer, so a live test runs on the running clock, and
    /// `start_live` refuses a paused one.
    ///
    /// A publisher the service uses itself is outside this record: one kept in its state, one an
    /// `after_startup` hook is handed or pairs, one from
    /// [`Bound::live`](ruststream_runtime::Bound::live) or
    /// [`RunningApp::publisher`](ruststream_runtime_tokio::RunningApp::publisher), and one a broker crate
    /// builds outside the runtime's pairing. The harness neither waits for its publishes nor lists
    /// them; a test sends its own input through `tb.broker::<B>().message(..)`.
    ///
    /// `subscriber(..)` and `settled(..)` read the same record in both modes.
    /// `published::<T>(name)` reads the broker's log in process and the harness's record live: the
    /// test's publishes onto that broker, and the app's publishes that went to it, each as the
    /// framework handed it over. A test that runs in both modes asserts on those. What only the
    /// broker does is asserted in process: a setting its publisher turns into a header or a prefix,
    /// and a transaction, which commits on the broker. What a broker moves on its own (a native
    /// delayed redelivery, its own dead-lettering) is asserted through the subscription that
    /// receives it. Each mode covers what the other cannot: in process the broker's server-side
    /// semantics are the transport's model of them, and live they are the server's own.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    ///         b.include(accept)
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
            on_shutdown,
            after_shutdown,
            shutdown_timeout,
            subscription_end,
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
            on_shutdown,
            after_shutdown,
            continuations,
            shutdown_timeout,
            subscription_end,
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
            on_shutdown,
            after_shutdown,
            shutdown_timeout,
            subscription_end,
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
            on_shutdown,
            after_shutdown,
            continuations,
            shutdown_timeout,
            subscription_end,
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
            on_shutdown,
            after_shutdown,
            continuations,
            shutdown_timeout,
            subscription_end,
            state,
        } = args;
        // A publisher the runtime pairs names the connected broker it was paired against; this
        // is how the harness knows which registration that is.
        coordinator.locate(entries.iter().map(|entry| entry.lifecycle.as_any()));
        let shutdown = Shutdown::with_subscription_end(subscription_end);
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
        let hooks = ShutdownHooks::bind(on_shutdown, after_shutdown, &state);
        Ok(Self {
            entries,
            mode,
            coordinator: coordinator.clone(),
            _state: PhantomData,
            shutdown,
            handles,
            hooks: Mutex::new(hooks),
            continuations,
            shutdown_timeout,
            hook_failures: HookFailures::new(coordinator),
        })
    }
    /// Shuts the harness down the way [`RunningApp::shutdown`](ruststream_runtime_tokio::RunningApp::shutdown)
    /// shuts the service down: the `on_shutdown` hooks, then the dispatch loops, the in-flight
    /// handlers and post-settle continuations (bounded by the app's shutdown timeout), the brokers
    /// in reverse registration order, and the `after_shutdown` hooks last. A failing shutdown hook
    /// is logged and the shutdown goes on.
    ///
    /// # Panics
    ///
    /// Raises again the panic a test's hook left (see
    /// [`SubscriberAssertions::on_call`](ruststream_runtime::testing::SubscriberAssertions::on_call)) once the handlers
    /// are done, so a hook's failed assertion fails the test however it ends. A harness dropped
    /// without a shutdown raises it as it goes.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError::Dispatch`] when a fail-fast failure tore the service down,
    /// [`RustStreamError::Shutdown`] when a broker fails to shut down, and
    /// [`RustStreamError::Join`] when a dispatch task panicked.
    pub async fn shutdown(self) -> Result<(), RustStreamError> {
        let teardown = Teardown {
            shutdown: self.shutdown,
            handles: self.handles,
            hooks: self
                .hooks
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
            brokers: self
                .entries
                .into_iter()
                .map(|entry| ConnectedEntry {
                    lifecycle: entry.lifecycle,
                    label: entry.label,
                })
                .collect(),
            shutdown_timeout: self.shutdown_timeout,
            continuations: self.continuations,
        };
        let stopped = teardown.run().await;
        // The handlers are done or aborted: the hooks they left waiting go with them.
        self.hook_failures.stop().await;
        // The handlers are done, so a failure a hook left is in by now.
        self.hook_failures.raise();
        stopped
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
    let until = timeout.map(|timeout| Instant::now() + timeout);
    for mut handle in handles {
        let Some(until) = until else {
            let _ = handle.await;
            continue;
        };
        if time::timeout_at(until, &mut handle).await.is_err() {
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
            let _ = time::timeout(timeout, continuations.wait()).await;
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
    on_shutdown: Vec<LifecycleHook<State>>,
    after_shutdown: Vec<LifecycleHook<State>>,
    continuations: TaskTracker,
    shutdown_timeout: Option<Duration>,
    subscription_end: SubscriptionEnd,
    state: Arc<State>,
}
