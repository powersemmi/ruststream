//! Running the service: startup sequence, signal handling and graceful shutdown.

use std::fmt;
use std::{future::Future, sync::Arc, time::Duration};

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::runtime::failure::ErrorShutdown;
use crate::runtime::lifecycle::{BoxError, BoxFuture, ConnectedLifecycle};

use super::health::{self, HealthProbe, HealthState};
use super::service::RegisteredBroker;
use super::{LifecycleHook, RustStream, RustStreamError};

/// A broker that finished [`Broker::connect`](crate::Broker::connect), held erased for the
/// consuming teardown, paired with its optional label.
pub(crate) struct ConnectedEntry {
    pub(crate) lifecycle: Box<dyn ConnectedLifecycle>,
    pub(crate) label: Option<String>,
}

/// A lifecycle hook with the state already bound, so [`RunningApp`] stays non-generic.
type BoundHook = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BoxError>> + Send>;

/// Binds the shared state into each hook, erasing the state type from the hook list.
fn bind_hooks<State: Send + Sync + 'static>(
    hooks: Vec<LifecycleHook<State>>,
    state: &Arc<State>,
) -> Vec<BoundHook> {
    hooks
        .into_iter()
        .map(|hook| {
            let state = Arc::clone(state);
            Box::new(move || hook(state)) as BoundHook
        })
        .collect()
}

// `run`/`run_until` are routinely driven from a multi-thread runtime (`tokio::spawn`, the CLI's
// `block_on`), so their futures must be `Send`: the shared state is held as `Arc<State>` across the
// startup awaits (needs `State: Sync`) and the global stack `Layers` is carried in `self` (needs `Layers: Send`).
// `State: 'static` is what every constructible app already satisfies (the `on_startup` producer
// returns the state from a `'static` boxed future); naming it here lets `start` box the shutdown
// hooks with the state bound in.
impl<Layers: Send, State: Send + Sync + 'static, Pipeline, Phase>
    RustStream<Layers, State, Pipeline, Phase>
{
    /// Runs the service until an interrupt (`SIGINT` / `SIGTERM`) is received, then shuts down
    /// gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a broker fails to connect, a subscription fails to open, a
    /// dispatch task panics, or a broker fails to shut down.
    pub async fn run(self) -> Result<(), RustStreamError> {
        self.run_until(wait_for_signal()).await
    }

    /// Runs the service until `shutdown` resolves, then shuts down gracefully.
    ///
    /// Use this instead of [`run`](Self::run) to drive shutdown from a caller-owned future (a
    /// name, a timeout, a test signal) rather than from process signals.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a broker fails to connect, a subscription fails to open, a
    /// dispatch task panics, or a broker fails to shut down.
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), RustStreamError>
    where
        F: Future<Output = ()> + Send,
    {
        let running = self.start().await?;
        tokio::select! {
            () = shutdown => info!(target: "ruststream::lifecycle", "shutdown signal received"),
            () = running.stopping() => {
                info!(target: "ruststream::lifecycle", "fail-fast shutdown triggered");
            }
        }
        running.shutdown().await
    }

    /// Starts the service in the background and hands back a [`RunningApp`] handle.
    ///
    /// Performs the same startup sequence as [`run`](Self::run) - the `on_startup` state
    /// producer, broker connects, subscription opens, `after_startup` hooks - and resolves once
    /// the service is running, so a startup failure surfaces here, before the caller starts
    /// accepting its own traffic. Installs no signal handlers: the caller decides what stops the
    /// service, by calling [`RunningApp::shutdown`].
    ///
    /// Use this to run the service beside another foreground server (an HTTP framework) in the
    /// same process; when the service is the whole process, [`run`](Self::run) /
    /// [`run_until`](Self::run_until) stay the simpler form.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if the state producer or an `after_startup` hook fails, a
    /// broker fails to connect, or a subscription fails to open. Brokers that had already
    /// connected are shut down (best effort, failures logged) before the error is returned, so
    /// a failed startup does not leak live connections.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "memory")]
    /// # async fn run() -> Result<(), ruststream::runtime::RustStreamError> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, RustStream};
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new());
    /// let running = app.start().await?;
    /// // ... serve HTTP in the foreground ...
    /// running.shutdown().await
    /// # }
    /// ```
    pub async fn start(self) -> Result<RunningApp, RustStreamError> {
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

        debug!(target: "ruststream::lifecycle", "producing application state");
        let state = state_init().await.map_err(RustStreamError::Startup)?;
        let state = Arc::new(state);

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

        let token = CancellationToken::new();
        // Shared with every dispatch task: a fail-fast failure records its reason here and cancels
        // the token, which both stops the loops and resolves `stopping()`.
        let error_shutdown = ErrorShutdown::new(token.clone());
        let mut handles = Vec::with_capacity(starters.len());
        for (starter, meta) in starters.into_iter().zip(handlers) {
            let handle = match starter(state.clone(), error_shutdown.clone(), token.clone()).await {
                Ok(handle) => handle,
                Err(err) => {
                    unwind_started(&token, handles, shutdown_timeout, connected).await;
                    return Err(RustStreamError::Subscribe(err));
                }
            };
            info!(
                target: "ruststream::dispatch",
                subscriber = %meta.name,
                input = meta.input_type,
                "subscriber started",
            );
            handles.push(handle);
        }

        if !after_startup.is_empty() {
            debug!(target: "ruststream::lifecycle", count = after_startup.len(), "running after_startup hooks");
        }
        for hook in after_startup {
            if let Err(err) = hook(Arc::clone(&state)).await {
                unwind_started(&token, handles, shutdown_timeout, connected).await;
                return Err(RustStreamError::Startup(err));
            }
        }

        info!(target: "ruststream::lifecycle", subscribers = handles.len(), "service running");

        let health = health::channel();
        // The watcher flips the probe on a fail-fast teardown even when nobody ever calls
        // `shutdown`: the process may stay alive serving healthz from a sibling task, which is
        // exactly when the probe matters.
        {
            let health = health.clone();
            let error_shutdown = error_shutdown.clone();
            let token = token.clone();
            tokio::spawn(async move {
                token.cancelled().await;
                if let Some(reason) = error_shutdown.peek_failure() {
                    let _ = health.send(HealthState::Failed { reason });
                }
            });
        }

        Ok(RunningApp {
            token,
            error_shutdown,
            handles,
            on_shutdown: bind_hooks(on_shutdown, &state),
            after_shutdown: bind_hooks(after_shutdown, &state),
            brokers: connected,
            shutdown_timeout,
            continuations,
            health,
        })
    }
}

/// A started service, handed out by [`RustStream::start`].
///
/// The handle owns the graceful teardown: dropping it without calling
/// [`shutdown`](Self::shutdown) detaches the service (dispatch tasks keep running, nothing is
/// drained), per the crate rule that destructors never block. The intended shape when running
/// beside a foreground server:
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() -> Result<(), ruststream::runtime::RustStreamError> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, RustStream};
///
/// let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new());
/// let running = app.start().await?;
/// // e.g. axum::serve(listener, router)
/// //     .with_graceful_shutdown(running.stopping())
/// //     .await?;
/// running.shutdown().await
/// # }
/// ```
#[must_use = "dropping the handle detaches the service without graceful shutdown"]
pub struct RunningApp {
    token: CancellationToken,
    error_shutdown: ErrorShutdown,
    handles: Vec<JoinHandle<()>>,
    on_shutdown: Vec<BoundHook>,
    after_shutdown: Vec<BoundHook>,
    brokers: Vec<ConnectedEntry>,
    shutdown_timeout: Option<Duration>,
    continuations: TaskTracker,
    health: watch::Sender<HealthState>,
}

impl fmt::Debug for RunningApp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunningApp")
            .field("subscribers", &self.handles.len())
            .field("brokers", &self.brokers.len())
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl RunningApp {
    /// Resolves when the service begins stopping on its own: a subscriber hit a fail-fast
    /// failure and tore the service down.
    ///
    /// The future is owned (`'static`), so it plugs directly into another server's graceful
    /// shutdown (for example axum's `with_graceful_shutdown`), stopping the host when the
    /// messaging side dies. It does not resolve on an orderly [`shutdown`](Self::shutdown) call -
    /// the caller drives that path itself.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the future loses nothing; a fresh call observes the same state.
    pub fn stopping(&self) -> impl Future<Output = ()> + Send + 'static {
        self.token.clone().cancelled_owned()
    }

    /// Hands out a [`HealthProbe`]: a cheap, cloneable view of the service's lifecycle state for
    /// a sibling task (typically an HTTP healthz endpoint).
    ///
    /// The probe outlives [`shutdown`](Self::shutdown) and keeps reporting the terminal state
    /// ([`HealthState::Stopped`], or [`HealthState::Failed`] with the fail-fast diagnostic), so
    /// an orchestrator stops routing traffic to a process whose messaging side died while
    /// another task keeps the process alive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "memory")]
    /// # async fn run() -> Result<(), ruststream::runtime::RustStreamError> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, HealthState, RustStream};
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new());
    /// let running = app.start().await?;
    /// let health = running.health();
    /// assert!(health.is_running());
    /// // hand `health` to the HTTP task's /healthz route, then later:
    /// running.shutdown().await?;
    /// assert_eq!(health.state(), HealthState::Stopped);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn health(&self) -> HealthProbe {
        HealthProbe::new(self.health.subscribe())
    }

    /// Pairs a [`Bound`](crate::runtime::Bound) token against its broker, for sending from a
    /// sibling task while the service runs (the third home of a publisher, next to reply
    /// wiring and [`Egress`](crate::runtime::Egress) injection).
    ///
    /// Sugar over [`Bound::live`](crate::runtime::Bound::live), anchored here because the handle
    /// existing is the witness that startup connected every registered broker.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`](crate::PairError) when the token's policy fails to pair.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, RustStream};
    ///
    /// let mut egress = None;
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         egress = Some(b.bind(MemoryPublish));
    ///     });
    /// let running = app.start().await?;
    /// let publisher = running.publisher(egress.take().expect("bound")).await?;
    /// // hand `publisher` to the sibling task (an outbox relay, a timer) ...
    /// # let _ = publisher;
    /// running.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    // Not `async fn`: the future must not capture `&self` (the handle holds non-Sync hook
    // boxes), and pairing only needs the token.
    pub fn publisher<B2, S>(
        &self,
        token: crate::runtime::Bound<B2, S>,
    ) -> impl Future<Output = Result<S::Live, crate::PairError>> + Send
    where
        B2: crate::Broker + 'static,
        S: crate::PublishPolicy<crate::Connected<B2>> + Send,
    {
        token.live()
    }

    /// Shuts the service down gracefully.
    ///
    /// Runs the `on_shutdown` hooks, stops the dispatch loops, drains in-flight handlers and
    /// post-settle continuations (bounded by the configured shutdown timeout), shuts the brokers
    /// down in reverse registration order, then runs the `after_shutdown` hooks.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a dispatch task panicked, a broker failed to shut down, or
    /// the service had already torn itself down on a fail-fast failure (surfaced as
    /// [`RustStreamError::Dispatch`] so the operator sees a non-zero exit, not a silent stop).
    pub async fn shutdown(self) -> Result<(), RustStreamError> {
        let Self {
            token,
            error_shutdown,
            handles,
            on_shutdown,
            after_shutdown,
            brokers,
            shutdown_timeout,
            continuations,
            health,
        } = self;

        let _ = health.send(HealthState::ShuttingDown);
        let outcome = teardown(
            token,
            handles,
            on_shutdown,
            after_shutdown,
            brokers,
            shutdown_timeout,
            continuations,
        )
        .await;
        match outcome {
            Ok(()) => {
                // A fail-fast failure tore the service down: surface it so an orchestrator
                // restarts the service and the operator sees a non-zero exit, not a silent stop.
                if let Some(reason) = error_shutdown.taken_failure() {
                    let _ = health.send(HealthState::Failed {
                        reason: reason.clone(),
                    });
                    return Err(RustStreamError::Dispatch(reason));
                }
                let _ = health.send(HealthState::Stopped);
                Ok(())
            }
            Err(err) => {
                let _ = health.send(HealthState::Failed {
                    reason: err.to_string(),
                });
                Err(err)
            }
        }
    }
}

/// Best-effort unwind of a startup that failed after dispatch tasks were spawned: stops the
/// tasks, then shuts the connected brokers down. Failures are logged, not returned, so the
/// original startup error stays the caller's answer.
async fn unwind_started(
    token: &CancellationToken,
    handles: Vec<JoinHandle<()>>,
    shutdown_timeout: Option<Duration>,
    brokers: Vec<ConnectedEntry>,
) {
    token.cancel();
    if let Err(err) = drain_handles(handles, shutdown_timeout).await {
        warn!(
            target: "ruststream::lifecycle",
            error = %err,
            "draining dispatch tasks failed during the startup unwind",
        );
    }
    unwind_connected(brokers).await;
}

/// Shuts down the brokers a failed startup had already connected, in reverse connect order,
/// logging shutdown failures instead of returning them.
async fn unwind_connected(brokers: Vec<ConnectedEntry>) {
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

/// The fallible half of the graceful teardown, factored out so [`RunningApp::shutdown`] can map
/// its outcome onto the health probe's terminal state in one place.
async fn teardown(
    token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    on_shutdown: Vec<BoundHook>,
    after_shutdown: Vec<BoundHook>,
    brokers: Vec<ConnectedEntry>,
    shutdown_timeout: Option<Duration>,
    continuations: TaskTracker,
) -> Result<(), RustStreamError> {
    for hook in on_shutdown {
        if let Err(err) = hook().await {
            warn!(target: "ruststream::lifecycle", error = %err, "on_shutdown hook failed");
        }
    }

    token.cancel();
    debug!(target: "ruststream::lifecycle", "draining in-flight handlers");
    drain_handles(handles, shutdown_timeout).await?;

    // Handlers have stopped, so no new post-settle continuations can be spawned: close the
    // tracker and drain the in-flight ones, bounded by the same shutdown timeout. They are
    // at-most-once, so timing one out only abandons follow-up work, never a settlement.
    drain_continuations(continuations, shutdown_timeout).await;

    for ConnectedEntry { lifecycle, label } in brokers.into_iter().rev() {
        let name = lifecycle.name();
        lifecycle
            .shutdown()
            .await
            .map_err(RustStreamError::Shutdown)?;
        debug!(
            target: "ruststream::lifecycle",
            broker = label.as_deref().unwrap_or(name),
            "broker shut down",
        );
    }

    for hook in after_shutdown {
        if let Err(err) = hook().await {
            warn!(target: "ruststream::lifecycle", error = %err, "after_shutdown hook failed");
        }
    }
    info!(target: "ruststream::lifecycle", "service stopped");
    Ok(())
}

/// Awaits all handler tasks, bounded by `timeout` if set. On timeout the remaining tasks are
/// aborted; without a timeout, a panicking task surfaces as [`RustStreamError::Join`].
async fn drain_handles(
    handles: Vec<JoinHandle<()>>,
    timeout: Option<Duration>,
) -> Result<(), RustStreamError> {
    let Some(timeout) = timeout else {
        for handle in handles {
            handle.await.map_err(RustStreamError::Join)?;
        }
        return Ok(());
    };

    let aborts: Vec<_> = handles.iter().map(JoinHandle::abort_handle).collect();
    if tokio::time::timeout(timeout, futures::future::join_all(handles))
        .await
        .is_err()
    {
        warn!(
            target: "ruststream::lifecycle",
            "graceful shutdown timed out; aborting in-flight handlers",
        );
        for abort in aborts {
            abort.abort();
        }
    }
    Ok(())
}

/// Closes the post-settle continuation tracker and waits for the in-flight continuations to finish,
/// bounded by `timeout` when set. On timeout the remaining continuations keep running detached (the
/// tracker does not own abort handles); they are at-most-once side effects, so abandoning them is
/// safe.
async fn drain_continuations(continuations: TaskTracker, timeout: Option<Duration>) {
    continuations.close();
    if continuations.is_empty() {
        return;
    }
    debug!(target: "ruststream::lifecycle", "draining post-settle continuations");
    match timeout {
        Some(timeout) => {
            if tokio::time::timeout(timeout, continuations.wait())
                .await
                .is_err()
            {
                warn!(
                    target: "ruststream::lifecycle",
                    "graceful shutdown timed out; abandoning in-flight continuations",
                );
            }
        }
        None => continuations.wait().await,
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
