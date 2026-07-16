//! Shared helpers for the integration tests.

use std::time::Duration;

/// Waits until `cond` holds, yielding between checks, but no longer than `timeout`.
///
/// The yield loop is the sanctioned no-sleep wait: in multi-thread mode the handler runs on
/// another worker and flips the observed state independently, so yielding is enough to let it
/// progress.
#[allow(dead_code)] // each test binary compiles its own copy and uses what it needs
pub(crate) async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(result.is_ok(), "condition not met within {timeout:?}");
}
