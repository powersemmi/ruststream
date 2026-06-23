//! Running the service: startup sequence, signal handling and graceful shutdown.

use std::{future::Future, sync::Arc, time::Duration};

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::runtime::failure::ErrorShutdown;

use super::{RustStream, RustStreamError};

// `run`/`run_until` are routinely driven from a multi-thread runtime (`tokio::spawn`, the CLI's
// `block_on`), so their futures must be `Send`: the shared state is held as `Arc<St>` across the
// startup awaits (needs `St: Sync`) and the global stack `L` is carried in `self` (needs `L: Send`).
impl<L: Send, St: Send + Sync> RustStream<L, St> {
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

        for broker in &brokers {
            broker.connect().await.map_err(RustStreamError::Connect)?;
            info!(target: "ruststream::lifecycle", broker = broker.name(), "broker connected");
        }

        let token = CancellationToken::new();
        // Shared with every dispatch task: a fail-fast failure records its reason here and cancels
        // the token, which both stops the loops and wakes the shutdown wait below.
        let error_shutdown = ErrorShutdown::new(token.clone());
        let mut handles = Vec::with_capacity(starters.len());
        for (starter, meta) in starters.into_iter().zip(handlers) {
            let handle = starter(state.clone(), error_shutdown.clone(), token.clone())
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

        if !after_startup.is_empty() {
            debug!(target: "ruststream::lifecycle", count = after_startup.len(), "running after_startup hooks");
        }
        for hook in after_startup {
            hook(Arc::clone(&state))
                .await
                .map_err(RustStreamError::Startup)?;
        }

        info!(target: "ruststream::lifecycle", subscribers = handles.len(), "service running");

        // Wake on either the caller's shutdown signal or a fail-fast cancellation from a dispatch
        // task, then tear the service down the same way for both.
        tokio::select! {
            () = shutdown => info!(target: "ruststream::lifecycle", "shutdown signal received"),
            () = token.cancelled() => {
                info!(target: "ruststream::lifecycle", "fail-fast shutdown triggered");
            }
        }

        for hook in on_shutdown {
            if let Err(err) = hook(Arc::clone(&state)).await {
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

        for broker in brokers.iter().rev() {
            broker.shutdown().await.map_err(RustStreamError::Shutdown)?;
            debug!(target: "ruststream::lifecycle", broker = broker.name(), "broker shut down");
        }

        for hook in after_shutdown {
            if let Err(err) = hook(Arc::clone(&state)).await {
                warn!(target: "ruststream::lifecycle", error = %err, "after_shutdown hook failed");
            }
        }
        info!(target: "ruststream::lifecycle", "service stopped");

        // A fail-fast failure tore the service down: surface it so an orchestrator restarts the
        // service and the operator sees a non-zero exit, not a silent stop.
        if let Some(reason) = error_shutdown.taken_failure() {
            return Err(RustStreamError::Dispatch(reason));
        }
        Ok(())
    }
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
