//! The graceful teardown of a started service, shared by [`RunningApp`](super::RunningApp) and the
//! `TestApp` harness so the two shut a service down the same way.

// The start and stop path runs on the task that awaits it and is never spawned.
#![allow(clippy::future_not_send)]

use std::{sync::Arc, time::Duration};

use ruststream_core::diag::{debug, info, warn};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::task::TaskTracker;

use crate::shutdown::Shutdown;
use crate::{BoxError, Clock, ConnectedLifecycle, LocalBoxFuture, Tokio};

use super::{LifecycleHook, RustStreamError};

/// A broker that finished [`Broker::connect`](crate::Broker::connect), held erased for the
/// consuming teardown, paired with its optional label.
// Machinery the harness crate drives, hidden from the docs; it holds erased closures and
// futures that have no useful `Debug`.
#[allow(missing_debug_implementations)]
pub struct ConnectedEntry {
    /// The connected broker, erased.
    pub lifecycle: Box<dyn ConnectedLifecycle>,
    /// The label it was registered under, if any.
    pub label: Option<String>,
}

/// A lifecycle hook with the state already bound, so the teardown stays non-generic.
pub(crate) type BoundHook =
    Box<dyn FnOnce() -> LocalBoxFuture<'static, Result<(), BoxError>> + Send>;

/// The app's shutdown hooks, with the state bound in.
// Machinery the harness crate drives, hidden from the docs; it holds erased closures and
// futures that have no useful `Debug`.
#[allow(missing_debug_implementations)]
pub struct ShutdownHooks {
    on_shutdown: Vec<BoundHook>,
    after_shutdown: Vec<BoundHook>,
}

impl ShutdownHooks {
    /// Binds the shared state into each hook, erasing the state type from the hook lists.
    #[doc(hidden)]
    pub fn bind<State: Send + Sync + 'static>(
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
// Machinery the harness crate drives, hidden from the docs; it holds erased closures and
// futures that have no useful `Debug`.
#[allow(missing_debug_implementations)]
pub struct Teardown {
    /// The service's shutdown signal.
    pub shutdown: Shutdown,
    /// The dispatch loops to join.
    pub handles: Vec<JoinHandle<()>>,
    /// The shutdown hooks the service registered.
    pub hooks: ShutdownHooks,
    /// The connected brokers, shut down last.
    pub brokers: Vec<ConnectedEntry>,
    /// How long shutdown waits for in-flight handlers.
    pub shutdown_timeout: Option<Duration>,
    /// The spawned continuations shutdown joins.
    pub continuations: TaskTracker,
}

impl Teardown {
    /// Shuts the service down gracefully: the `on_shutdown` hooks, the dispatch loops, the
    /// in-flight handlers and post-settle continuations (bounded by the shutdown timeout), the
    /// brokers in reverse registration order, then the `after_shutdown` hooks.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError::Dispatch`] if the service had already torn itself down on a
    /// fail-fast failure, a panic outside a handler included: that failure is the answer even
    /// when a broker then fails to shut down, which is logged. Otherwise a task of the runner that
    /// ended abnormally is [`RustStreamError::Join`], and a broker that failed to shut down is
    /// [`RustStreamError::Shutdown`].
    ///
    /// # Cancel safety
    ///
    /// Each stage takes its items out of `self` one at a time, so a `run` that is dropped
    /// mid-way leaves the items it had not reached in place, and a second `run` finishes them.
    /// The harness relies on this to complete a teardown it had to abandon.
    #[doc(hidden)]
    pub async fn run(&mut self) -> Result<(), RustStreamError> {
        let Self {
            shutdown,
            handles,
            hooks,
            brokers,
            shutdown_timeout,
            continuations,
        } = self;
        let shutdown_timeout = *shutdown_timeout;

        shutdown.begin();
        run_hooks(&mut hooks.on_shutdown, "on_shutdown").await;

        shutdown.cancel();
        debug!(target: "ruststream::lifecycle", "draining in-flight handlers");
        // Every later step still runs when one fails: a dispatch loop that panicked or a broker
        // that refuses to close must not leave the other brokers open or skip `after_shutdown`.
        // The first failure is the one returned.
        let drained = drain_handles(handles, shutdown_timeout).await;

        // Handlers have stopped, so no new post-settle continuations can be spawned: close the
        // tracker and drain the in-flight ones, bounded by the same shutdown timeout. They are
        // at-most-once, so timing one out only abandons follow-up work, never a settlement.
        drain_continuations(continuations.clone(), shutdown_timeout).await;

        let closed = close_brokers(brokers).await;

        run_hooks(&mut hooks.after_shutdown, "after_shutdown").await;
        info!(target: "ruststream::lifecycle", "service stopped");

        // A fail-fast failure tore the service down: it is the answer, so an orchestrator
        // restarts the service and the operator sees why it stopped, not a silent stop. What the
        // teardown met on the way is no news next to it: a loop that panicked recorded that
        // failure under its subscription's name, so its join error is the same panic without
        // the name, and a broker that refused to close is logged where it refused.
        if let Some(reason) = shutdown.taken_failure() {
            return Err(RustStreamError::Dispatch(reason));
        }
        drained?;
        closed
    }
}

/// Shuts the brokers down in reverse registration order. Every broker gets its turn even after
/// one fails; the first failure is the one returned.
async fn close_brokers(brokers: &mut Vec<ConnectedEntry>) -> Result<(), RustStreamError> {
    let mut closed = Ok(());
    // Taken off the list one at a time, so a teardown dropped mid-way keeps the brokers it had
    // not reached.
    while let Some(ConnectedEntry { lifecycle, label }) = brokers.pop() {
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
async fn run_hooks(hooks: &mut Vec<BoundHook>, phase: &'static str) {
    while !hooks.is_empty() {
        let hook = hooks.remove(0);
        if let Err(err) = hook().await {
            warn!(target: "ruststream::lifecycle", error = %err, "{phase} hook failed");
        }
    }
}

/// Awaits all handler tasks, bounded by `timeout` if set. On timeout the remaining tasks are
/// aborted and awaited, so a loop is gone, not merely told to go, when this returns. A task that
/// panicked surfaces as [`RustStreamError::Join`], with a timeout or without one.
///
/// A loop leaves the list only once it has finished, so a drain that is dropped mid-wait keeps
/// the loops it had not seen finish, and a second drain aborts them.
pub(crate) async fn drain_handles(
    handles: &mut Vec<JoinHandle<()>>,
    timeout: Option<Duration>,
) -> Result<(), RustStreamError> {
    let Some(timeout) = timeout else {
        // Every loop is awaited, so one that panicked does not leave the others running while
        // the brokers close; the first panic is the one returned.
        let mut joined = Ok(());
        while !handles.is_empty() {
            if let Err(err) = (&mut handles[0]).await
                && joined.is_ok()
            {
                joined = Err(RustStreamError::Join(err));
            }
            handles.remove(0);
        }
        return joined;
    };

    let until = Tokio.now() + timeout;
    let mut timed_out = false;
    let mut joined = Ok(());
    while !handles.is_empty() {
        let handle = &mut handles[0];
        if let Ok(outcome) = time::timeout_at(until, &mut *handle).await {
            // A loop that finished before the deadline reports a panic as the untimed path does;
            // a cancelled one says nothing.
            if let Err(err) = outcome
                && err.is_panic()
                && joined.is_ok()
            {
                joined = Err(RustStreamError::Join(err));
            }
            handles.remove(0);
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
        handles.remove(0);
    }
    joined
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
            let drained = drain_handles(&mut vec![panicked, gated], None).await;
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

    /// A dispatch loop that panics is reported by `run` with a shutdown timeout set, as it is
    /// without one.
    #[tokio::test]
    async fn a_panicked_loop_fails_run_under_a_shutdown_timeout() {
        let panicked = tokio::spawn(async { panic!("dispatch loop panicked") });
        let mut teardown = Teardown {
            shutdown: Shutdown::new(),
            handles: vec![panicked],
            hooks: ShutdownHooks::bind::<()>(Vec::new(), Vec::new(), &Arc::new(())),
            brokers: Vec::new(),
            shutdown_timeout: Some(Duration::from_secs(5)),
            continuations: TaskTracker::new(),
        };

        let result = teardown.run().await;

        assert!(
            matches!(result, Err(RustStreamError::Join(_))),
            "got: {result:?}"
        );
    }
}
