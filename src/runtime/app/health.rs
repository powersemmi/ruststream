//! The [`HealthProbe`]: a cloneable, watch-backed view of the running service's lifecycle state.
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

/// A cheap, cloneable handle reporting the service's [`HealthState`].
///
/// Obtained from [`RunningApp::health`](super::RunningApp::health); backed by a
/// [`watch`](tokio::sync::watch) channel written only on lifecycle transitions, so reading is a
/// lock-free borrow and nothing runs on the per-delivery hot path. The probe outlives
/// [`shutdown`](super::RunningApp::shutdown) (which consumes the app handle), so a sibling HTTP
/// task keeps answering with the terminal state.
///
/// Watch semantics: observers always see the latest state; rapid transitions (`ShuttingDown`
/// immediately followed by `Stopped`) may coalesce, so poll [`state`](Self::state) for snapshots
/// and use [`changed`](Self::changed) only as a wake-up.
///
/// Dropping the [`RunningApp`](super::RunningApp) without calling `shutdown` detaches the service
/// (per the crate rule that destructors never block); a probe then keeps reporting the last
/// observed state, and [`changed`](Self::changed) resolves only if a fail-fast failure still
/// flips the state.
#[derive(Debug, Clone)]
pub struct HealthProbe {
    rx: watch::Receiver<HealthState>,
}

impl HealthProbe {
    pub(super) fn new(rx: watch::Receiver<HealthState>) -> Self {
        Self { rx }
    }

    /// A snapshot of the current state.
    #[must_use]
    pub fn state(&self) -> HealthState {
        self.rx.borrow().clone()
    }

    /// Whether the service is currently running (the healthy answer for a liveness endpoint).
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.rx.borrow().is_running()
    }

    /// Waits for the next state transition and returns the new state.
    ///
    /// If the service was detached (the app handle dropped without `shutdown`) and no further
    /// transition can arrive, the future stays pending forever rather than spinning the caller's
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

/// The write half the running app and its fail-fast watcher drive; probes are subscribed off it,
/// so the public surface stays read-only.
pub(super) fn channel() -> watch::Sender<HealthState> {
    watch::channel(HealthState::Running).0
}
