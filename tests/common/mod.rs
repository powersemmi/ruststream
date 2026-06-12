//! Shared helpers for the integration tests.

use std::time::Duration;

use tokio::sync::Notify;

/// Waits until a handler signals `notify`, but no longer than one retry interval.
///
/// Used in publish-retry loops: subscriptions open inside `run()`, so a message published too
/// early is lost and the signal never comes. The bounded wait lets the caller publish again
/// instead of deadlocking, while still waking immediately on the happy path.
pub(crate) async fn handler_signal(notify: &Notify) {
    let _ = tokio::time::timeout(Duration::from_millis(10), notify.notified()).await;
}
