//! The [`HealthProbe`]: a cloneable, watch-backed view of the running service's lifecycle state
//! and of how many of its subscriptions are consuming.
//!
//! The standard deployment shape runs the messaging service beside another tokio task - typically
//! an HTTP server with a `/healthz` route. When the service fail-fasts, the process stays alive
//! because of that sibling task, and the probe is what lets its endpoint report "the app died,
//! stop routing traffic here" instead of a permanent 200.

use std::future::pending;

use ruststream_runtime::{HealthState, SubscriptionCount};

pub use ruststream_runtime::SubscriptionEnd;
use tokio::sync::watch;

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
