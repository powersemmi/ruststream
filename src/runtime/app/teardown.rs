//! The graceful teardown of a started service, shared by [`RunningApp`](super::RunningApp) and the
//! `TestApp` harness so the two shut a service down the same way.

use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Instant;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::runtime::lifecycle::{BoxError, BoxFuture, ConnectedLifecycle};
use crate::runtime::shutdown::Shutdown;

use super::{LifecycleHook, RustStreamError};

/// A broker that finished [`Broker::connect`](crate::Broker::connect), held erased for the
/// consuming teardown, paired with its optional label.
pub(crate) struct ConnectedEntry {
    pub(crate) lifecycle: Box<dyn ConnectedLifecycle>,
    pub(crate) label: Option<String>,
}

/// A lifecycle hook with the state already bound, so the teardown stays non-generic.
pub(crate) type BoundHook = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BoxError>> + Send>;

/// The app's shutdown hooks, with the state bound in.
pub(crate) struct ShutdownHooks {
    on_shutdown: Vec<BoundHook>,
    after_shutdown: Vec<BoundHook>,
}

impl ShutdownHooks {
    /// Binds the shared state into each hook, erasing the state type from the hook lists.
    pub(crate) fn bind<State: Send + Sync + 'static>(
        on_shutdown: Vec<LifecycleHook<State>>,
        after_shutdown: Vec<LifecycleHook<State>>,
        state: &Arc<State>,
    ) -> Self {
        Self {
            on_shutdown: bind_hooks(on_shutdown, state),
            after_shutdown: bind_hooks(after_shutdown, state),
        }
    }
}

fn bind_hooks<State: Send + Sync + 'static>(
    hooks: Vec<LifecycleHook<State>>,
    state: &Arc<State>,
) -> Vec<BoundHook> {
    hooks
        .into_iter()
        .map(|hook| -> BoundHook {
            let state = Arc::clone(state);
            Box::new(move || hook(state))
        })
        .collect()
}

/// Everything a started service tears down, in the order it tears it down.
pub(crate) struct Teardown {
    pub(crate) shutdown: Shutdown,
    pub(crate) handles: Vec<JoinHandle<()>>,
    pub(crate) hooks: ShutdownHooks,
    pub(crate) brokers: Vec<ConnectedEntry>,
    pub(crate) shutdown_timeout: Option<Duration>,
    pub(crate) continuations: TaskTracker,
}

impl Teardown {
    /// Shuts the service down gracefully: the `on_shutdown` hooks, the dispatch loops, the
    /// in-flight handlers and post-settle continuations (bounded by the shutdown timeout), the
    /// brokers in reverse registration order, then the `after_shutdown` hooks.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a dispatch task panicked, a broker failed to shut down, or
    /// the service had already torn itself down on a fail-fast failure
    /// ([`RustStreamError::Dispatch`]).
    pub(crate) async fn run(self) -> Result<(), RustStreamError> {
        let Self {
            shutdown,
            handles,
            hooks,
            brokers,
            shutdown_timeout,
            continuations,
        } = self;

        shutdown.begin();
        run_hooks(hooks.on_shutdown, "on_shutdown").await;

        shutdown.cancel();
        debug!(target: "ruststream::lifecycle", "draining in-flight handlers");
        // Every later step still runs when one fails: a dispatch loop that panicked or a broker
        // that refuses to close must not leave the other brokers open or skip `after_shutdown`.
        // The first failure is the one returned.
        let drained = drain_handles(handles, shutdown_timeout).await;

        // Handlers have stopped, so no new post-settle continuations can be spawned: close the
        // tracker and drain the in-flight ones, bounded by the same shutdown timeout. They are
        // at-most-once, so timing one out only abandons follow-up work, never a settlement.
        drain_continuations(continuations, shutdown_timeout).await;

        let closed = close_brokers(brokers).await;

        run_hooks(hooks.after_shutdown, "after_shutdown").await;
        info!(target: "ruststream::lifecycle", "service stopped");

        drained?;
        closed?;

        // A fail-fast failure tore the service down: surface it so an orchestrator restarts the
        // service and the operator sees a non-zero exit, not a silent stop.
        shutdown
            .taken_failure()
            .map_or(Ok(()), |reason| Err(RustStreamError::Dispatch(reason)))
    }
}

/// Shuts the brokers down in reverse registration order. Every broker gets its turn even after
/// one fails; the first failure is the one returned.
async fn close_brokers(brokers: Vec<ConnectedEntry>) -> Result<(), RustStreamError> {
    let mut closed = Ok(());
    for ConnectedEntry { lifecycle, label } in brokers.into_iter().rev() {
        let name = lifecycle.name();
        let broker = label.as_deref().unwrap_or(name);
        match lifecycle.shutdown().await {
            Ok(()) => debug!(target: "ruststream::lifecycle", broker, "broker shut down"),
            Err(err) => {
                warn!(
                    target: "ruststream::lifecycle",
                    broker,
                    error = %err,
                    "broker shutdown failed",
                );
                if closed.is_ok() {
                    closed = Err(RustStreamError::Shutdown(err));
                }
            }
        }
    }
    closed
}

/// Runs one list of shutdown hooks in order. A failing hook is logged and does not stop the
/// shutdown: the service is going down either way, and the hooks after it still get their turn.
async fn run_hooks(hooks: Vec<BoundHook>, phase: &'static str) {
    for hook in hooks {
        if let Err(err) = hook().await {
            warn!(target: "ruststream::lifecycle", error = %err, "{phase} hook failed");
        }
    }
}

/// Awaits all handler tasks, bounded by `timeout` if set. On timeout the remaining tasks are
/// aborted and awaited, so a loop is gone, not merely told to go, when this returns; without a
/// timeout, a panicking task surfaces as [`RustStreamError::Join`].
pub(crate) async fn drain_handles(
    handles: Vec<JoinHandle<()>>,
    timeout: Option<Duration>,
) -> Result<(), RustStreamError> {
    let Some(timeout) = timeout else {
        // Every loop is awaited, so one that panicked does not leave the others running while
        // the brokers close; the first panic is the one returned.
        let mut joined = Ok(());
        for handle in handles {
            if let Err(err) = handle.await
                && joined.is_ok()
            {
                joined = Err(RustStreamError::Join(err));
            }
        }
        return joined;
    };

    let until = Instant::now() + timeout;
    let mut timed_out = false;
    for mut handle in handles {
        if time::timeout_at(until, &mut handle).await.is_ok() {
            continue;
        }
        if !timed_out {
            timed_out = true;
            warn!(
                target: "ruststream::lifecycle",
                "graceful shutdown timed out; aborting in-flight handlers",
            );
        }
        handle.abort();
        // The aborted loop ends at its next await; its outcome (cancelled) says nothing more.
        let _ = handle.await;
    }
    Ok(())
}

/// Closes the post-settle continuation tracker and waits for the in-flight continuations to finish,
/// bounded by `timeout` when set. On timeout the remaining continuations keep running detached (the
/// tracker does not own abort handles); they are at-most-once side effects, so abandoning them is
/// safe.
pub(crate) async fn drain_continuations(continuations: TaskTracker, timeout: Option<Duration>) {
    continuations.close();
    if continuations.is_empty() {
        return;
    }
    debug!(target: "ruststream::lifecycle", "draining post-settle continuations");
    match timeout {
        Some(timeout) => {
            if time::timeout(timeout, continuations.wait()).await.is_err() {
                warn!(
                    target: "ruststream::lifecycle",
                    "graceful shutdown timed out; abandoning in-flight continuations",
                );
            }
        }
        None => continuations.wait().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::oneshot;
    use tokio::task;

    use super::*;

    /// A loop that panicked does not end the drain early: the drain returns its error only once
    /// every other loop has finished, so the brokers never close under a running handler.
    #[tokio::test]
    async fn a_panicked_loop_is_reported_after_the_others_finish() {
        let finished = Arc::new(AtomicBool::new(false));
        let (release, gate) = oneshot::channel::<()>();
        let panicked = tokio::spawn(async { panic!("dispatch loop panicked") });
        let flag = Arc::clone(&finished);
        let gated = tokio::spawn(async move {
            let _ = gate.await;
            flag.store(true, Ordering::SeqCst);
        });

        let drain = async {
            let drained = drain_handles(vec![panicked, gated], None).await;
            (drained, finished.load(Ordering::SeqCst))
        };
        let releaser = async {
            task::yield_now().await;
            task::yield_now().await;
            let _ = release.send(());
        };
        let ((drained, gated_finished), ()) = tokio::join!(drain, releaser);

        assert!(
            matches!(drained, Err(RustStreamError::Join(_))),
            "got: {drained:?}"
        );
        assert!(
            gated_finished,
            "the drain returned while a loop was still running"
        );
    }
}
