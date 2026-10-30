//! Running the service: startup sequence, signal handling and graceful shutdown.

use std::{fmt, sync::Arc, time::Duration};

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

#[cfg(unix)]
use tokio::signal::ctrl_c;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::HandlerMetadata;
use crate::bindable::Bound;
use crate::shutdown::Shutdown;
use crate::{Broker, Connected, PairError, PublishPolicy};

use super::health::{HealthProbe, HealthState};
use super::service::RegisteredBroker;
use super::teardown::{
    ConnectedEntry, ShutdownHooks, Teardown, drain_continuations, drain_handles,
};
use super::{LifecycleHook, RustStream, RustStreamError, Starter, StateInit};

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
            subscription_end,
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

        let state = produce_state(state_init).await?;
        let connected = connect_brokers(brokers).await?;

        // Shared with every dispatch task: a fail-fast failure records its reason here and raises
        // the signal, which both stops the loops and resolves `stopping()`.
        let shutdown = Shutdown::with_subscription_end(subscription_end);
        let mut handles = Vec::with_capacity(starters.len());
        let mut started =
            start_subscribers(starters, handlers, &state, &shutdown, &mut handles).await;
        if started.is_ok() {
            started = run_after_startup(after_startup, &state).await;
        }
        if let Err(err) = started {
            unwind_started(
                &shutdown,
                handles,
                shutdown_timeout,
                connected,
                continuations,
            )
            .await;
            return Err(err);
        }

        info!(target: "ruststream::lifecycle", subscribers = handles.len(), "service running");

        watch_for_failure(shutdown.clone());

        Ok(RunningApp {
            teardown: Teardown {
                shutdown,
                handles,
                hooks: ShutdownHooks::bind(on_shutdown, after_shutdown, &state),
                brokers: connected,
                shutdown_timeout,
                continuations,
            },
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
    teardown: Teardown,
}

impl fmt::Debug for RunningApp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunningApp")
            .field("subscribers", &self.teardown.handles.len())
            .field("brokers", &self.teardown.brokers.len())
            .field("shutdown_timeout", &self.teardown.shutdown_timeout)
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
        self.teardown.shutdown.token().clone().cancelled_owned()
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
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use axum::Router;
    /// use axum::extract::State;
    /// use axum::http::StatusCode;
    /// use axum::routing::get;
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, HealthProbe, RustStream};
    /// use tokio::net::TcpListener;
    ///
    /// async fn healthz(State(health): State<HealthProbe>) -> StatusCode {
    ///     if health.is_running() {
    ///         StatusCode::OK
    ///     } else {
    ///         StatusCode::SERVICE_UNAVAILABLE
    ///     }
    /// }
    ///
    /// let app =
    ///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new());
    /// let running = app.start().await?;
    /// let routes = Router::new()
    ///     .route("/healthz", get(healthz))
    ///     .with_state(running.health());
    /// axum::serve(TcpListener::bind("0.0.0.0:8080").await?, routes)
    ///     .with_graceful_shutdown(running.stopping())
    ///     .await?;
    /// running.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn health(&self) -> HealthProbe {
        self.teardown.shutdown.health().probe()
    }

    /// Pairs a [`Bound`](crate::Bound) token against its broker, for sending from a
    /// sibling task while the service runs (the third home of a publisher, next to reply
    /// wiring and [`Out`](crate::Out) injection).
    ///
    /// Sugar over [`Bound::live`](crate::Bound::live), anchored here because the handle
    /// existing is the witness that startup connected every registered broker.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when the token's policy fails to pair.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "json")]
    /// # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::prelude::*;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "orders")]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// let broker = MemoryBroker::new().bindable();
    /// let egress = broker.bind(Publish);
    /// let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| b);
    /// let running = app.start().await?;
    ///
    /// // The HTTP task owns this publisher and sends what its requests carry.
    /// let publisher = running.publisher(egress).await?;
    /// publisher.message(&Order { id: 7 }).publish().await?;
    ///
    /// running.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    // Not `async fn`: the future must not capture `&self` (the handle holds non-Sync hook
    // boxes), and pairing only needs the token.
    pub fn publisher<B2, S>(
        &self,
        token: Bound<B2, S>,
    ) -> impl Future<Output = Result<S::Live, PairError>> + Send
    where
        B2: Broker + 'static,
        S: PublishPolicy<Connected<B2>> + Send,
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
    /// A failure does not cut the teardown short: every broker is still shut down and the
    /// `after_shutdown` hooks still run before the first failure is returned.
    pub async fn shutdown(self) -> Result<(), RustStreamError> {
        let Self { teardown } = self;
        // The teardown consumes its signal; the probes read the report the signal shares.
        let shutdown = teardown.shutdown.clone();
        let health = shutdown.health();
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

/// Opens every subscription and spawns its dispatch task, collecting the task handles into
/// `handles`. The handles stay with the caller on a failure, which is what the unwind drains.
async fn start_subscribers<State: Send + Sync>(
    starters: Vec<Starter<State>>,
    metadata: Vec<HandlerMetadata>,
    state: &Arc<State>,
    shutdown: &Shutdown,
    handles: &mut Vec<JoinHandle<()>>,
) -> Result<(), RustStreamError> {
    for (starter, meta) in starters.into_iter().zip(metadata) {
        let handle = starter(Arc::clone(state), shutdown.clone())
            .await
            .map_err(RustStreamError::Subscribe)?;
        info!(
            target: "ruststream::dispatch",
            subscriber = %meta.name,
            input = meta.input_type,
            "subscriber started",
        );
        handles.push(handle);
    }
    Ok(())
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
///
fn watch_for_failure(shutdown: Shutdown) {
    tokio::spawn(async move {
        shutdown.cancelled().await;
        if let Some(reason) = shutdown.peek_failure() {
            shutdown.health().transition(HealthState::Failed { reason });
        }
    });
}

/// Best-effort unwind of a startup that failed after dispatch tasks were spawned: stops the
/// tasks, drains their post-settle continuations, then shuts the connected brokers down - the
/// same ordering the graceful teardown keeps, so continuations never run against a closed
/// broker. Failures are logged, not returned, so the original startup error stays the caller's
/// answer.
async fn unwind_started(
    shutdown: &Shutdown,
    handles: Vec<JoinHandle<()>>,
    shutdown_timeout: Option<Duration>,
    brokers: Vec<ConnectedEntry>,
    continuations: TaskTracker,
) {
    shutdown.cancel();
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

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            let _ = ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
