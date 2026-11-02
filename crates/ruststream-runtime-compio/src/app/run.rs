//! Running the service: startup sequence, the cores and graceful shutdown.

// The start and stop path runs on the task that awaits it and is never spawned.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::fmt;
use std::future::pending;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use compio::runtime;
use futures::future::{self, Either};
use tracing::{debug, info, warn};

#[cfg(feature = "testing")]
use crate::ConnectedLifecycle;
use crate::bindable::Bound;
use crate::dispatch::{Fabric, Layout};
use crate::time;
use crate::{
    Connected, Cores, HandlerMetadata, PairError, PublishPolicy, SendBroker, ShutdownWait, Stop,
    Tasks,
};

use super::health::{Consuming, HealthProbe, HealthReport};
use super::service::RegisteredBroker;
use super::teardown::{
    ConnectedEntry, Held, Loop, ShutdownHooks, Teardown, drain_continuations, drain_handles,
};
use super::{HealthState, Launch, LifecycleHook, Mounted, RustStream, RustStreamError, StateInit};

// The whole service is handed to core 0 across a thread boundary when the cores start, so
// `run` and `run_until` need the app to be `Send`: the layer stack and the publish pipeline too.
impl<Layers: Send, State: Send + Sync + 'static, Pipeline: Send, Phase>
    RustStream<Layers, State, Pipeline, Phase>
{
    /// Runs the service on `cores` until the process is asked to end (`SIGINT` or `SIGTERM`, unless
    /// `cores` was built [`without_signals`](Cores::without_signals)), then shuts down gracefully.
    ///
    /// Core 0 runs on the calling thread and every further core on a thread of its own, each with a
    /// compio runtime; the call returns when all of them have ended.
    ///
    /// # Errors
    ///
    /// [`RustStreamError`] if the mounts do not fit the cores, a broker fails to connect, a
    /// subscription fails to open, a dispatch task panics, a core fails, or a broker fails to shut
    /// down.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime::AppInfo;
    /// use ruststream_runtime_compio::{Cores, RustStream, RustStreamError};
    ///
    /// fn main() -> Result<(), RustStreamError> {
    ///     let cores = Cores::new(NonZeroUsize::MIN);
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(LocalMemoryBroker::<4>::new(), |b| b)
    ///         .run(cores)
    /// }
    /// ```
    pub fn run(self, cores: Cores) -> Result<(), RustStreamError> {
        self.run_until(cores, pending())
    }

    /// Runs the service on `cores` until `shutdown` resolves, then shuts down gracefully.
    ///
    /// Use this instead of [`run`](Self::run) to drive shutdown from a caller-owned future (a
    /// name, a timeout, a test signal) rather than from process signals. `shutdown` runs on core 0
    /// beside the service. A fail-fast failure, a core that fails, and the process signals of
    /// `cores` end the run too.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] under the same conditions as [`run`](Self::run).
    ///
    /// # Panics
    ///
    /// Resumes the panic of core 0, after the other cores have ended.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::future::ready;
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime::AppInfo;
    /// use ruststream_runtime_compio::{Cores, RustStream, RustStreamError};
    ///
    /// # fn main() -> Result<(), RustStreamError> {
    /// let cores = Cores::new(NonZeroUsize::MIN).without_signals();
    /// // The future that ends the run is anything: a channel, a deadline, a test's notification.
    /// RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///     .with_broker(LocalMemoryBroker::<4>::new(), |b| b)
    ///     .run_until(cores, ready(()))
    /// # }
    /// ```
    pub fn run_until<Signal>(self, cores: Cores, shutdown: Signal) -> Result<(), RustStreamError>
    where
        Signal: Future<Output = ()> + Send,
    {
        let count = cores.count().get();
        // Placed before any thread starts, so a mount that does not fit is an error of the call.
        let layout = self.layout(count)?;
        let stop = Stop::new(self.subscription_end);
        let fabric = Fabric::open(count);
        let timeout = self.shutdown_timeout;
        // Raised by core 0 once its teardown is done: the other cores keep their runtimes, and the
        // continuations on them, until then.
        let released = Stop::default();
        let job = Mutex::new(Some((self, shutdown, layout)));

        let ended = cores.run(&stop, async |core, stop| {
            if core.is_main() {
                let (app, shutdown, layout) = job
                    .lock()
                    .ok()
                    .and_then(|mut job| job.take())
                    .expect("core 0 runs once");
                let _release = Release(released.clone());
                let outcome = app.drive(shutdown, layout, stop, fabric.clone()).await;
                Ok::<_, Infallible>(Some(outcome))
            } else {
                serve_core(&fabric, core.index(), &stop, timeout).await;
                released.cancelled().await;
                Ok(None)
            }
        });
        match ended {
            Ok(outcomes) => outcomes.into_iter().flatten().next().unwrap_or(Ok(())),
            Err(failure) => Err(RustStreamError::Cores(failure)),
        }
    }

    /// Starts on core 0, runs until `shutdown` resolves or the service stops itself, and shuts
    /// down.
    async fn drive<Signal>(
        self,
        shutdown: Signal,
        layout: Layout,
        stop: Stop,
        fabric: Fabric,
    ) -> Result<(), RustStreamError>
    where
        Signal: Future<Output = ()>,
    {
        let running = self
            .start_on(layout, stop, fabric, Wiring::production())
            .await?;
        let stopping = running.stopping();
        if let Either::Right(((), _)) = future::select(pin!(shutdown), pin!(stopping)).await {
            running.say_why_it_stops();
        } else {
            info!(target: "ruststream::lifecycle", "shutdown signal received");
        }
        running.shutdown().await
    }
}

impl<Layers: Send, State: Send + Sync + 'static, Pipeline, Phase>
    RustStream<Layers, State, Pipeline, Phase>
{
    /// Starts the service in the background of the calling runtime and hands back a
    /// [`RunningApp`] handle.
    ///
    /// The calling runtime is the only core: the service starts as [`run`](Self::run) does (the
    /// `on_startup` state producer, broker connects, subscription opens, `after_startup` hooks)
    /// and resolves once it is running, so a startup failure surfaces here. `workers(n)` mounts
    /// run their workers on this core; a `threads(n)` mount needs cores of its own and is refused
    /// with [`RustStreamError::Layout`]. Installs no signal handlers: the caller decides what
    /// stops the service, by calling [`RunningApp::shutdown`].
    ///
    /// Call it from a compio runtime, for a service that runs beside other work of that runtime or
    /// under a test.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if the mounts do not fit one core, the state producer or an
    /// `after_startup` hook fails, a broker fails to connect, or a subscription fails to open.
    /// Brokers that had already connected are shut down (best effort, failures logged) before the
    /// error is returned, so a failed startup does not leak live connections.
    ///
    /// # Examples
    ///
    /// ```
    /// use compio::runtime::Runtime;
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime::AppInfo;
    /// use ruststream_runtime_compio::RustStream;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// Runtime::new()?.block_on(async {
    ///     let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///         .register_broker(LocalMemoryBroker::<4>::new());
    ///     let running = app.start().await?;
    ///     // ... the rest of the runtime's work, while the service runs ...
    ///     running.shutdown().await?;
    ///     Ok(())
    /// })
    /// # }
    /// ```
    pub async fn start(self) -> Result<RunningApp, RustStreamError> {
        let layout = self.layout(1)?;
        let stop = Stop::new(self.subscription_end);
        self.start_on(layout, stop, Fabric::open(1), Wiring::production())
            .await
    }

    /// The startup sequence on core 0, over the cores `layout` was planned for.
    pub(super) async fn start_on(
        self,
        layout: Layout,
        stop: Stop,
        fabric: Fabric,
        wiring: Wiring<State>,
    ) -> Result<RunningApp, RustStreamError> {
        let Self {
            info,
            brokers,
            starters,
            handlers,
            state_init,
            after_startup,
            on_shutdown,
            after_shutdown,
            shutdown_timeout,
            continuations,
            ..
        } = self;

        info!(
            target: "ruststream::lifecycle",
            service = %info.title,
            version = %info.version,
            brokers = brokers.len(),
            subscribers = starters.len(),
            "starting service",
        );

        #[cfg_attr(not(feature = "testing"), allow(unused_variables))]
        let (state, connected, harness) = connect_wired(wiring, state_init, brokers).await?;

        let health = HealthReport::new();
        // Core 0 serves its own mailbox beside the app, as every other core does for its own.
        let mut handles: Vec<Loop> = vec![{
            let (fabric, stop) = (fabric.clone(), stop.clone());
            runtime::spawn(async move {
                fabric.serve(0, &stop).await;
                Ok(())
            })
        }];
        let mut started = start_subscribers(
            starters,
            handlers,
            &state,
            &Placed {
                layout: &layout,
                stop: &stop,
                fabric: &fabric,
                health: &health,
                #[cfg(feature = "testing")]
                harness,
            },
            &mut handles,
        )
        .await;
        if started.is_ok() {
            started = run_after_startup(after_startup, &state).await;
        }
        if let Err(err) = started {
            unwind_started(&stop, handles, shutdown_timeout, connected, &continuations).await;
            return Err(err);
        }

        info!(target: "ruststream::lifecycle", subscribers = handles.len() - 1, "service running");

        watch_for_failure(stop.clone(), health.clone());

        Ok(RunningApp {
            teardown: Teardown {
                stop,
                handles: Held::new(handles),
                hooks: ShutdownHooks::bind(on_shutdown, after_shutdown, &state),
                brokers: connected,
                shutdown_timeout,
                continuations,
            },
            health,
        })
    }
}

/// A started service, handed out by [`RustStream::start`].
///
/// The handle owns the graceful teardown: dropping it without calling
/// [`shutdown`](Self::shutdown) detaches the service (the loops keep running on their cores,
/// nothing is drained and no hook runs), per the crate rule that destructors never block.
///
/// # Examples
///
/// ```
/// use compio::runtime::Runtime;
/// use ruststream_memory::local::LocalMemoryBroker;
/// use ruststream_runtime::AppInfo;
/// use ruststream_runtime_compio::RustStream;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// Runtime::new()?.block_on(async {
///     let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(LocalMemoryBroker::<4>::new());
///     let running = app.start().await?;
///     // A sibling task reads the probe while the service runs.
///     assert!(running.health().is_running());
///     running.shutdown().await?;
///     Ok(())
/// })
/// # }
/// ```
#[must_use = "dropping the handle detaches the service without graceful shutdown"]
pub struct RunningApp {
    teardown: Teardown,
    health: HealthReport,
}

impl fmt::Debug for RunningApp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunningApp")
            .field("brokers", &self.teardown.brokers.len())
            .field("shutdown_timeout", &self.teardown.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl RunningApp {
    /// Resolves when the service begins stopping on its own: a subscriber hit a fail-fast
    /// failure, a core failed, or the process was asked to end.
    ///
    /// The future is owned, so it plugs into another server's graceful shutdown, stopping the
    /// host when the messaging side dies.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the future loses nothing; a fresh call observes the same state.
    pub fn stopping(&self) -> impl Future<Output = ()> + Send + 'static {
        let stop = self.teardown.stop.clone();
        async move { stop.cancelled().await }
    }

    /// Says what stopped the service on its own: a failure, or the process being asked to end.
    fn say_why_it_stops(&self) {
        if self.teardown.stop.failure().is_some() {
            info!(target: "ruststream::lifecycle", "fail-fast shutdown triggered");
        } else {
            info!(target: "ruststream::lifecycle", "the process was asked to end");
        }
    }

    /// The brokers the app connected, in registration order, with their labels: what the test
    /// harness addresses by type.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn connected(
        &self,
    ) -> impl Iterator<Item = (Option<&str>, &(dyn ConnectedLifecycle + 'static))> {
        self.teardown
            .brokers
            .iter()
            .map(|entry| (entry.label.as_deref(), &*entry.lifecycle))
    }

    /// Hands out a [`HealthProbe`]: a cheap, cloneable view of the service's lifecycle state for
    /// a sibling task (typically an HTTP healthz endpoint).
    ///
    /// The probe outlives [`shutdown`](Self::shutdown) and keeps reporting the terminal state
    /// ([`HealthState::Stopped`], or [`HealthState::Failed`] with the fail-fast diagnostic).
    #[must_use]
    pub fn health(&self) -> HealthProbe {
        self.health.probe()
    }

    /// Pairs a [`Bound`] token against its broker, for sending from a sibling task while the
    /// service runs.
    ///
    /// Sugar over [`Bound::live`], anchored here because the handle existing is the witness that
    /// startup connected every registered broker.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when the token's policy fails to pair.
    // Not `async fn`: the future must not capture `&self`, and pairing only needs the token.
    pub fn publisher<B2, S>(
        &self,
        token: Bound<B2, S>,
    ) -> impl Future<Output = Result<S::Live, PairError>>
    where
        B2: SendBroker,
        S: PublishPolicy<Connected<B2>>,
    {
        token.live()
    }

    /// Shuts the service down gracefully.
    ///
    /// Runs the `on_shutdown` hooks, stops the dispatch loops, drains in-flight handlers and
    /// post-settle continuations (bounded by the configured shutdown timeout; a handler still
    /// running at the cap is cancelled), shuts the brokers down in reverse registration order,
    /// then runs the `after_shutdown` hooks.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a dispatch task panicked, a broker failed to shut down, or
    /// the service had already torn itself down on a fail-fast failure (surfaced as
    /// [`RustStreamError::Dispatch`] so the operator sees a non-zero exit, not a silent stop).
    /// A failure does not cut the teardown short: every broker is still shut down and the
    /// `after_shutdown` hooks still run before the first failure is returned.
    pub async fn shutdown(self) -> Result<(), RustStreamError> {
        let Self { teardown, health } = self;
        health.transition(HealthState::ShuttingDown);
        let outcome = teardown.run().await;
        let terminal = match &outcome {
            Ok(()) => HealthState::Stopped,
            // The fail-fast diagnostic is the probe's reason as it is, without the error's prefix.
            Err(RustStreamError::Dispatch(reason)) => HealthState::Failed {
                reason: reason.clone(),
            },
            Err(err) => HealthState::Failed {
                reason: err.to_string(),
            },
        };
        health.transition(terminal);
        outcome
    }
}

/// Lets the other cores go when core 0's part is over, however it ends.
struct Release(Stop);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// What a core other than core 0 does: serves its mailbox until the app stops, and gives the
/// lanes it runs until `timeout` after that to finish.
async fn serve_core(fabric: &Fabric, core: usize, stop: &Stop, timeout: Option<Duration>) {
    let serving = pin!(fabric.serve(core, stop));
    let Some(timeout) = timeout else {
        serving.await;
        return;
    };
    let cap = pin!(async {
        stop.cancelled().await;
        time::sleep(timeout).await;
    });
    if let Either::Right(((), _)) = future::select(serving, cap).await {
        warn!(
            target: "ruststream::lifecycle",
            core,
            "graceful shutdown timed out; aborting in-flight handlers",
        );
    }
}

/// Where the mounts run and what they report to: the arguments the subscription starts share.
struct Placed<'app> {
    layout: &'app Layout,
    stop: &'app Stop,
    fabric: &'app Fabric,
    health: &'app HealthReport,
    /// The hooks of the harness the app runs under, where a test drives it.
    #[cfg(feature = "testing")]
    harness: HarnessSlot,
}

/// How the brokers connect and the state is made: as production does, or, under a test, in
/// process.
#[cfg(feature = "testing")]
pub(super) enum Wiring<State> {
    Production,
    InProcess(super::InProcessStart<State>),
}

#[cfg(feature = "testing")]
impl<State> Wiring<State> {
    pub(super) const fn production() -> Self {
        Self::Production
    }
}

/// How the brokers connect and the state is made.
#[cfg(not(feature = "testing"))]
pub(super) struct Wiring<State>(std::marker::PhantomData<fn() -> State>);

#[cfg(not(feature = "testing"))]
impl<State> Wiring<State> {
    pub(super) const fn production() -> Self {
        Self(std::marker::PhantomData)
    }
}

/// What the harness the app runs under hands the mounts.
#[cfg(feature = "testing")]
type HarnessSlot = Option<super::Harness>;
#[cfg(not(feature = "testing"))]
type HarnessSlot = ();

/// Produces the state and connects the brokers the way `wiring` says.
#[cfg(feature = "testing")]
async fn connect_wired<State>(
    wiring: Wiring<State>,
    state_init: StateInit<State>,
    brokers: Vec<RegisteredBroker>,
) -> Result<(Arc<State>, Vec<ConnectedEntry>, HarnessSlot), RustStreamError> {
    match wiring {
        Wiring::Production => Ok((
            produce_state(state_init).await?,
            connect_brokers(brokers).await?,
            None,
        )),
        Wiring::InProcess(start) => {
            let (state, connected, hooks) = start.connect(state_init, brokers).await?;
            Ok((state, connected, Some(hooks)))
        }
    }
}

/// Produces the state and connects the brokers.
#[cfg(not(feature = "testing"))]
async fn connect_wired<State>(
    _wiring: Wiring<State>,
    state_init: StateInit<State>,
    brokers: Vec<RegisteredBroker>,
) -> Result<(Arc<State>, Vec<ConnectedEntry>, HarnessSlot), RustStreamError> {
    Ok((
        produce_state(state_init).await?,
        connect_brokers(brokers).await?,
        (),
    ))
}

/// Produces the application state through the `on_startup` producer chain, once, before any
/// broker connects.
async fn produce_state<State>(state_init: StateInit<State>) -> Result<Arc<State>, RustStreamError> {
    debug!(target: "ruststream::lifecycle", "producing application state");
    let state = state_init().await.map_err(RustStreamError::Startup)?;
    Ok(Arc::new(state))
}

/// Connects the registered brokers in registration order. A failed connect shuts down the
/// brokers connected before it, so a failed startup leaks no live connection.
async fn connect_brokers(
    brokers: Vec<RegisteredBroker>,
) -> Result<Vec<ConnectedEntry>, RustStreamError> {
    let mut connected = Vec::with_capacity(brokers.len());
    for RegisteredBroker { lifecycle, label } in brokers {
        let lifecycle = match lifecycle.connect().await {
            Ok(lifecycle) => lifecycle,
            Err(err) => {
                unwind_connected(connected).await;
                return Err(RustStreamError::Connect(err));
            }
        };
        info!(
            target: "ruststream::lifecycle",
            broker = label.as_deref().unwrap_or_else(|| lifecycle.name()),
            "broker connected",
        );
        connected.push(ConnectedEntry { lifecycle, label });
    }
    Ok(connected)
}

/// Opens every subscription and starts its loop, collecting the loop handles into `handles`. The
/// handles stay with the caller on a failure, which is what the unwind drains.
async fn start_subscribers<State: Send + Sync>(
    starters: Vec<Mounted<State>>,
    metadata: Vec<HandlerMetadata>,
    state: &Arc<State>,
    placed: &Placed<'_>,
    handles: &mut Vec<Loop>,
) -> Result<(), RustStreamError> {
    for (index, (mounted, meta)) in starters.into_iter().zip(metadata).enumerate() {
        let launch = Launch {
            stop: placed.stop.clone(),
            fabric: placed.fabric.clone(),
            cores: placed.layout.cores(index).to_vec(),
            #[cfg(feature = "testing")]
            harness: placed.harness.clone(),
        };
        let handle = (mounted.start)(Arc::clone(state), launch)
            .await
            .map_err(RustStreamError::Subscribe)?;
        info!(
            target: "ruststream::dispatch",
            subscriber = %meta.name,
            input = meta.input_type,
            "subscriber started",
        );
        handles.push(counted(handle, placed.health.consuming()));
    }
    Ok(())
}

/// Holds `handle` in a task that carries the subscription's place in the health count, so the
/// count drops with the loop however it ends. Dropping or cancelling the returned handle cancels
/// the loop.
fn counted(handle: runtime::JoinHandle<()>, consuming: Consuming) -> Loop {
    runtime::spawn(async move {
        let _consuming = consuming;
        handle.await
    })
}

/// Runs the `after_startup` hooks in registration order, stopping at the first failure.
async fn run_after_startup<State: Send + Sync>(
    hooks: Vec<LifecycleHook<State>>,
    state: &Arc<State>,
) -> Result<(), RustStreamError> {
    if !hooks.is_empty() {
        debug!(target: "ruststream::lifecycle", count = hooks.len(), "running after_startup hooks");
    }
    for hook in hooks {
        hook(Arc::clone(state))
            .await
            .map_err(RustStreamError::Startup)?;
    }
    Ok(())
}

/// Reports a fail-fast teardown on the health probe even when nobody ever calls
/// [`RunningApp::shutdown`]: the process may stay alive serving healthz from a sibling task, which
/// is exactly when the probe matters.
fn watch_for_failure(stop: Stop, health: HealthReport) {
    runtime::spawn(async move {
        stop.cancelled().await;
        if let Some(reason) = stop.failure() {
            health.transition(HealthState::Failed { reason });
        }
    })
    .detach();
}

/// Best-effort unwind of a startup that failed after loops were started: stops them, drains their
/// post-settle continuations, then shuts the connected brokers down - the same ordering the
/// graceful teardown keeps, so continuations never run against a closed broker. Failures are
/// logged, not returned, so the original startup error stays the caller's answer.
async fn unwind_started(
    stop: &Stop,
    handles: Vec<Loop>,
    shutdown_timeout: Option<Duration>,
    brokers: Vec<ConnectedEntry>,
    continuations: &Tasks,
) {
    stop.cancel();
    if let Err(err) = drain_handles(handles, shutdown_timeout).await {
        warn!(
            target: "ruststream::lifecycle",
            error = %err,
            "draining dispatch tasks failed during the startup unwind",
        );
    }
    drain_continuations(continuations, shutdown_timeout).await;
    unwind_connected(brokers).await;
}

/// Shuts down the brokers a failed startup had already connected, in reverse connect order,
/// logging shutdown failures instead of returning them.
pub(super) async fn unwind_connected(brokers: Vec<ConnectedEntry>) {
    for ConnectedEntry { lifecycle, label } in brokers.into_iter().rev() {
        let name = lifecycle.name();
        match lifecycle.shutdown().await {
            Ok(()) => debug!(
                target: "ruststream::lifecycle",
                broker = label.as_deref().unwrap_or(name),
                "broker shut down after a startup failure",
            ),
            Err(err) => warn!(
                target: "ruststream::lifecycle",
                broker = label.as_deref().unwrap_or(name),
                error = %err,
                "broker shutdown failed during the startup unwind",
            ),
        }
    }
}
