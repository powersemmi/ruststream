//! The [`HealthProbe`]: a cloneable, watch-backed view of the running service's lifecycle state
//! and of how many of its subscriptions are consuming.
//!
//! The standard deployment shape runs the messaging service beside another tokio task - typically
//! an HTTP server with a `/healthz` route. When the service fail-fasts, the process stays alive
//! because of that sibling task, and the probe is what lets its endpoint report "the app died,
//! stop routing traffic here" instead of a permanent 200.

use std::future::pending;

use tokio::sync::watch;

/// The lifecycle state reported by a [`HealthProbe`].
///
/// [`RustStream::start`](super::RustStream::start) is the readiness gate (it resolves only after
/// subscriptions are open), so the probe covers the post-startup half of the lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HealthState {
    /// Subscriptions are open and the dispatch loops are running.
    Running,
    /// An orderly [`shutdown`](super::RunningApp::shutdown) is in progress.
    ShuttingDown,
    /// The service shut down cleanly.
    Stopped,
    /// The service tore itself down on a fail-fast failure, or teardown itself failed.
    Failed {
        /// The diagnostic the fail-fast path recorded (subscription and cause), the same string
        /// [`run`](super::RustStream::run) returns inside
        /// [`RustStreamError::Dispatch`](super::RustStreamError::Dispatch).
        reason: String,
    },
}

impl HealthState {
    /// Whether the state reports a live, dispatching service.
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

/// How many of the service's subscriptions are consuming, as a [`HealthProbe`] reports it.
///
/// A subscription counts once, whatever its worker count. It is active from the moment its
/// dispatch loop starts until the loop ends: on shutdown, or when its stream ends while the
/// service runs (see [`SubscriptionEnd`]). Whether a service with fewer active subscriptions than
/// it started is healthy is the service's own call.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{HealthProbe, RustStreamError, SubscriptionCount};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// /// Healthy while the service runs and every subscription it started still reads its queue.
/// fn healthy(health: &HealthProbe) -> bool {
///     let SubscriptionCount {
///         active, started, ..
///     } = health.subscriptions();
///     health.is_running() && active == started
/// }
///
/// pub async fn run() -> Result<(), RustStreamError> {
///     let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         MemoryBroker::new(),
///         |b| {
///             b.include(accept)
///         },
///     );
///     let running = app.start().await?;
///     tracing::info!(healthy = healthy(&running.health()), "service started");
///     running.shutdown().await
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionCount {
    /// The subscriptions whose dispatch loop is running.
    pub active: usize,
    /// The subscriptions the service started.
    pub started: usize,
}

/// What a subscription's stream ending while the service runs does, set with
/// [`RustStream::on_subscription_end`](super::RustStream::on_subscription_end).
///
/// A stream can end without a shutdown: the server cancels a consumer when its queue is deleted,
/// a broker's reconnection policy gives up, or a broker is configured not to reconnect. An end
/// during shutdown is part of the shutdown and is never reported.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::SubscriptionEnd;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// /// The service is useless without its queue, so it stops when the queue stops.
/// pub fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .on_subscription_end(SubscriptionEnd::FailFast)
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(accept)
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionEnd {
    /// The loop logs the end and exits; the rest of the service runs on. The subscription leaves
    /// the active count of [`HealthProbe::subscriptions`].
    #[default]
    Log,
    /// The end is a fail-fast failure: an error naming the subscription, a graceful shutdown,
    /// [`run`](super::RustStream::run) returning
    /// [`RustStreamError::Dispatch`](super::RustStreamError::Dispatch), and the probe reporting
    /// [`HealthState::Failed`].
    FailFast,
}

/// What the probe reads: the lifecycle state and the subscription count, one value, so a wake-up
/// covers both.
#[derive(Debug)]
pub(crate) struct Health {
    state: HealthState,
    subscriptions: SubscriptionCount,
}

/// A cheap, cloneable handle reporting the service's [`HealthState`] and how many of its
/// subscriptions are consuming.
///
/// Obtained from [`RunningApp::health`](super::RunningApp::health); backed by a
/// [`watch`](tokio::sync::watch) channel written only on lifecycle transitions and when a
/// subscription's loop starts or ends, so reading is a lock-free borrow and nothing runs on the
/// per-delivery hot path. The probe outlives [`shutdown`](super::RunningApp::shutdown) (which
/// consumes the app handle), so a sibling HTTP task keeps answering with the terminal state.
///
/// Watch semantics: observers always see the latest state; rapid transitions (`ShuttingDown`
/// immediately followed by `Stopped`) may coalesce, so poll [`state`](Self::state) for snapshots
/// and use [`changed`](Self::changed) only as a wake-up.
///
/// Dropping the [`RunningApp`](super::RunningApp) without calling `shutdown` detaches the service
/// (per the crate rule that destructors never block); a probe then keeps reporting the last
/// observed state, and [`changed`](Self::changed) resolves only if a fail-fast failure still
/// flips the state or a subscription still ends.
#[derive(Debug, Clone)]
pub struct HealthProbe {
    rx: watch::Receiver<Health>,
}

impl HealthProbe {
    /// A snapshot of the current state.
    #[must_use]
    pub fn state(&self) -> HealthState {
        self.rx.borrow().state.clone()
    }

    /// Whether the service is currently running (the healthy answer for a liveness endpoint).
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.rx.borrow().state.is_running()
    }

    /// How many subscriptions are consuming, and how many the service started.
    ///
    /// A subscription whose stream ended while the service runs leaves the active count, under
    /// the default [`SubscriptionEnd::Log`]. The lifecycle state stays
    /// [`Running`](HealthState::Running): the service decides what a partly working service
    /// reports outside.
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
    /// /// Ready while every subscription the service started still consumes.
    /// async fn readyz(State(health): State<HealthProbe>) -> StatusCode {
    ///     let count = health.subscriptions();
    ///     if health.is_running() && count.active == count.started {
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
    ///     .route("/readyz", get(readyz))
    ///     .with_state(running.health());
    /// axum::serve(TcpListener::bind("0.0.0.0:8080").await?, routes)
    ///     .with_graceful_shutdown(running.stopping())
    ///     .await?;
    /// running.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn subscriptions(&self) -> SubscriptionCount {
        self.rx.borrow().subscriptions
    }

    /// Waits for the next change, a lifecycle transition or a subscription starting or ending,
    /// and returns the state.
    ///
    /// If the service was detached (the app handle dropped without `shutdown`) and no further
    /// change can arrive, the future stays pending forever rather than spinning the caller's
    /// loop; pair it with [`state`](Self::state) when a snapshot is enough.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the future loses nothing; a fresh call observes the same channel.
    pub async fn changed(&mut self) -> HealthState {
        if self.rx.changed().await.is_ok() {
            return self.state();
        }
        // The sender is gone without a final transition: the state is final, park forever.
        pending().await
    }
}

/// The write half the running app, its fail-fast watcher and the dispatch loops drive; probes
/// are subscribed off it, so the public surface stays read-only.
///
/// Every write uses `send_modify`, which keeps the value when no probe is subscribed yet (where
/// `send` drops it): a probe taken after a change must still observe it.
#[derive(Debug)]
pub(crate) struct HealthReport(watch::Sender<Health>);

impl HealthReport {
    /// A running service with no subscription started yet.
    pub(crate) fn new() -> Self {
        Self(
            watch::channel(Health {
                state: HealthState::Running,
                subscriptions: SubscriptionCount::default(),
            })
            .0,
        )
    }

    /// A probe on this report.
    pub(crate) fn probe(&self) -> HealthProbe {
        HealthProbe {
            rx: self.0.subscribe(),
        }
    }

    /// Moves the lifecycle state to `state`.
    pub(crate) fn transition(&self, state: HealthState) {
        self.0.send_modify(|health| health.state = state);
    }

    /// A subscription's dispatch loop started.
    pub(crate) fn subscription_started(&self) {
        self.0.send_modify(|health| {
            health.subscriptions.started += 1;
            health.subscriptions.active += 1;
        });
    }

    /// A subscription's dispatch loop ended.
    pub(crate) fn subscription_ended(&self) {
        self.0
            .send_modify(|health| health.subscriptions.active -= 1);
    }
}
