//! The app's shutdown signal: the flag a running loop reads, the wait a parked one parks on, and
//! the failure that raised it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tracing::error;

/// What every dispatch loop stops on, in the two forms a loop asks for, plus the reason where a
/// failure raised it.
///
/// A turn asks twice: for the flag before it touches the stream, and for the wait where the
/// stream has nothing. The two cannot be one thing: a subscription that always has a record
/// ready never reaches the wait, so the flag is its only exit, and a parked subscription needs a
/// registration with a waiter list, which a flag cannot give it.
///
/// [`CancellationToken::is_cancelled`] answers the flag by taking the token tree's mutex, which a
/// loop pays on every delivery. The [`AtomicBool`] beside the token answers it with a relaxed load
/// instead, and the token keeps the wait. The flag rides the allocation the failure slot makes, so
/// an app pays no block for it.
///
/// Both ends belong to the runtime: [`cancel`](Self::cancel) and [`signal`](Self::signal) are the
/// only ways to raise one, so the flag and the wait cannot come apart. Cloning shares all of it,
/// which is how any dispatch task tears the whole service down.
#[derive(Clone, Debug, Default)]
pub(crate) struct Shutdown {
    token: CancellationToken,
    state: Arc<State>,
}

/// What every holder of a [`Shutdown`] shares: the flag, and the first failure's description.
#[derive(Debug, Default)]
struct State {
    cancelled: AtomicBool,
    failure: Mutex<Option<String>>,
}

impl Shutdown {
    /// A signal nobody has raised yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Raises the signal for every holder of this handle, with no failure behind it: the orderly
    /// teardown.
    pub(crate) fn cancel(&self) {
        // Relaxed: the flag carries no data of its own, and it is set before the token, so a
        // holder that observes the cancellation observes the flag as well. A holder that reads
        // the flag late loses nothing either, because the wait below is the authoritative exit.
        self.state.cancelled.store(true, Ordering::Relaxed);
        self.token.cancel();
    }

    /// Records `reason` (only the first wins), logs it against `subscription` and raises the
    /// signal, starting the graceful teardown. Idempotent: a second call after a failure is
    /// already recorded only re-raises the signal.
    pub(crate) fn signal(&self, subscription: &str, reason: &str) {
        error!(
            target: "ruststream::dispatch",
            subscription = %subscription,
            reason = %reason,
            "fail-fast: a dispatch failure is tearing the service down",
        );
        // Keep the lock only long enough to record the first failure; never held across an await.
        if let Ok(mut slot) = self.state.failure.lock() {
            slot.get_or_insert_with(|| format!("{subscription}: {reason}"));
        }
        self.cancel();
    }

    /// Whether the signal has been raised. This is the read a dispatch loop makes per delivery.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Relaxed)
    }

    /// Resolves once the signal is raised.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: the registration a parked poll made with the token's waiter list outlives
    /// that poll, and a raised signal stays raised, so a later call observes the same state.
    pub(crate) fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.token.cancelled()
    }

    /// The token itself, for the one surface that hands a shutdown wait to user code
    /// ([`RunningApp::stopping`](super::RunningApp::stopping)).
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Takes the first recorded failure description, if any. Read by the run loop once the
    /// service has drained, to decide whether to return an error.
    pub(crate) fn taken_failure(&self) -> Option<String> {
        self.state
            .failure
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
    }

    /// Reads the first recorded failure description without taking it: the health-probe watcher
    /// reports it without racing the run loop's consuming read, and the test harness reports
    /// `run_result` more than once.
    pub(crate) fn peek_failure(&self) -> Option<String> {
        self.state.failure.lock().ok().and_then(|slot| slot.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::Shutdown;

    #[tokio::test]
    async fn the_flag_and_the_wait_never_disagree() {
        let shutdown = Shutdown::new();
        assert!(!shutdown.is_cancelled());

        let watcher = shutdown.clone();
        shutdown.cancel();

        assert!(watcher.is_cancelled(), "the flag answers every holder");
        watcher.cancelled().await;
    }

    #[test]
    fn the_first_failure_is_the_one_reported() {
        let shutdown = Shutdown::new();
        assert!(!shutdown.is_cancelled());

        shutdown.signal("orders.inbound", "handler panicked");
        assert!(shutdown.is_cancelled());

        // The second failure does not overwrite the first.
        shutdown.signal("other", "second");
        assert_eq!(
            shutdown.peek_failure().as_deref(),
            Some("orders.inbound: handler panicked")
        );
        assert_eq!(
            shutdown.taken_failure().as_deref(),
            Some("orders.inbound: handler panicked")
        );
        // Taking it clears the slot.
        assert_eq!(shutdown.taken_failure(), None);
    }
}
