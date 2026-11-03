//! The graceful teardown of a started service, the order [`RunningApp::shutdown`] keeps.
//!
//! [`RunningApp::shutdown`]: super::RunningApp::shutdown

// The start and stop path runs on the task that awaits it and is never spawned.
#![allow(clippy::future_not_send)]

use std::any::Any;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use compio::runtime::{JoinError, JoinHandle};
use futures::future::{self, Either};
use ruststream_core::diag::{debug, info, warn};
use ruststream_runtime::Clock;

use crate::dispatch::Fabric;
use crate::{BoxError, CompioClock, ConnectedLifecycle, LocalBoxFuture, Stop, Tasks};

use super::{LifecycleHook, RustStreamError};

/// A loop or a task of core 0 that the teardown joins: its outcome, or why it did not end.
pub(crate) type Loop = JoinHandle<Result<(), JoinError>>;

/// The loops of a started service, which keep running if the handle that holds them is dropped.
///
/// compio ends a task whose handle is dropped; a started service is detached instead, as the
/// tokio runner's is, and only a shutdown ends it.
pub(crate) struct Held(Vec<Loop>);

impl Held {
    pub(crate) fn new(loops: Vec<Loop>) -> Self {
        Self(loops)
    }

    /// The loops, for the teardown to join; one leaves the list only once it is joined.
    fn loops(&mut self) -> &mut Vec<Loop> {
        &mut self.0
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        for held in self.0.drain(..) {
            held.detach();
        }
    }
}

/// A broker that finished [`Broker::connect`](crate::Broker::connect), held erased for the
/// consuming teardown, paired with its optional label.
pub(crate) struct ConnectedEntry {
    /// The connected broker, erased.
    pub(crate) lifecycle: Box<dyn ConnectedLifecycle>,
    /// The label it was registered under, if any.
    pub(crate) label: Option<String>,
}

/// A lifecycle hook with the state already bound, so the teardown stays non-generic.
type BoundHook = Box<dyn FnOnce() -> LocalBoxFuture<'static, Result<(), BoxError>> + Send>;

/// The app's shutdown hooks, with the state bound in.
pub(crate) struct ShutdownHooks {
    on_shutdown: Vec<BoundHook>,
    after_shutdown: Vec<BoundHook>,
}

impl ShutdownHooks {
    /// No hooks: what a startup that failed unwinds with.
    pub(crate) const fn none() -> Self {
        Self {
            on_shutdown: Vec::new(),
            after_shutdown: Vec::new(),
        }
    }

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
    /// The service's stop signal.
    pub(crate) stop: Stop,
    /// The dispatch loops, and the task serving core 0's mailbox, to join.
    pub(crate) handles: Held,
    /// The shutdown hooks the service registered.
    pub(crate) hooks: ShutdownHooks,
    /// The connected brokers, shut down last.
    pub(crate) brokers: Vec<ConnectedEntry>,
    /// How long shutdown waits for in-flight handlers.
    pub(crate) shutdown_timeout: Option<Duration>,
    /// The work deliveries left behind, which shutdown waits for.
    pub(crate) continuations: Tasks,
    /// The workers on the other cores, which are gone before the brokers close.
    pub(crate) fabric: Fabric,
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
    ///
    /// # Cancel safety
    ///
    /// Each stage takes its items out of `self` one at a time, so a `run` that is dropped
    /// mid-way leaves the items it had not reached in place, and a second `run` finishes them.
    /// The test harness relies on this to complete a teardown it had to abandon.
    pub(crate) async fn run(&mut self) -> Result<(), RustStreamError> {
        let Self {
            stop,
            handles,
            hooks,
            brokers,
            shutdown_timeout,
            continuations,
            fabric,
        } = self;
        let shutdown_timeout = *shutdown_timeout;

        stop.begin();
        run_hooks(&mut hooks.on_shutdown, "on_shutdown").await;

        stop.cancel();
        debug!(target: "ruststream::lifecycle", "draining in-flight handlers");
        // Every later step still runs when one fails: a dispatch loop that panicked or a broker
        // that refuses to close must not leave the other brokers open or skip `after_shutdown`.
        // The first failure is the one returned.
        let drained = drain_handles(handles.loops(), shutdown_timeout).await;
        // The loops of core 0 are over, but a worker on another core is told to go by its own
        // core's cap, so wait for it to be dropped: a handler must not outlive the brokers.
        fabric.workers_gone().await;

        // Handlers have stopped, so no new post-settle continuations can be spawned: wait for
        // the in-flight ones, on every core, bounded by the same shutdown timeout. They are
        // at-most-once, so timing one out only abandons follow-up work, never a settlement.
        drain_continuations(continuations, shutdown_timeout).await;

        let closed = close_brokers(brokers).await;

        run_hooks(&mut hooks.after_shutdown, "after_shutdown").await;
        info!(target: "ruststream::lifecycle", "service stopped");

        drained?;
        closed?;

        // A fail-fast failure tore the service down: surface it so an orchestrator restarts the
        // service and the operator sees a non-zero exit, not a silent stop.
        stop.failure()
            .map_or(Ok(()), |reason| Err(RustStreamError::Dispatch(reason)))
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

/// Awaits `work` until `deadline`, or without a bound where there is none. `None` is the deadline
/// passing first.
pub(crate) async fn within<Work>(deadline: Option<Instant>, work: Work) -> Option<Work::Output>
where
    Work: Future,
{
    let Some(deadline) = deadline else {
        return Some(work.await);
    };
    match future::select(pin!(work), pin!(CompioClock.sleep_until(deadline))).await {
        Either::Left((output, _)) => Some(output),
        Either::Right(((), _)) => None,
    }
}

/// The deadline `timeout` from now on the app's clock; none where the wait is not bounded.
pub(crate) fn deadline_after(timeout: Option<Duration>) -> Option<Instant> {
    let now = CompioClock.now();
    timeout.map(|timeout| now + timeout)
}

/// Why a loop did not end well, in words.
fn failure_of(error: &JoinError) -> String {
    match error {
        JoinError::Panicked(payload) => {
            let payload: &(dyn Any + Send) = &**payload;
            let message = payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned());
            message.map_or_else(
                || "the task panicked".to_owned(),
                |message| format!("the task panicked: {message}"),
            )
        }
        JoinError::Cancelled => "the task was cancelled".to_owned(),
    }
}

/// Awaits all loops, bounded by `timeout` if set. On timeout the remaining ones are cancelled and
/// awaited, so a loop is gone, not merely told to go, when this returns. A loop that panicked is
/// reported as [`RustStreamError::Join`] once every other loop has ended.
///
/// A loop leaves the list only once it has ended, so a drain that is dropped mid-wait keeps the
/// loops it had not seen end, and a second drain cancels them.
pub(crate) async fn drain_handles(
    handles: &mut Vec<Loop>,
    timeout: Option<Duration>,
) -> Result<(), RustStreamError> {
    let until = deadline_after(timeout);
    let mut joined = Ok(());
    let mut timed_out = false;
    while !handles.is_empty() {
        let Some(ended) = within(until, &mut handles[0]).await else {
            if !timed_out {
                timed_out = true;
                warn!(
                    target: "ruststream::lifecycle",
                    "graceful shutdown timed out; aborting in-flight handlers",
                );
            }
            // The cancelled loop ends at its next await; its outcome says nothing more.
            let _ = handles.remove(0).cancel().await;
            continue;
        };
        drop(handles.remove(0));
        if let Err(error) = ended.and_then(|outcome| outcome)
            && joined.is_ok()
            && !matches!(error, JoinError::Cancelled)
        {
            joined = Err(RustStreamError::Join(failure_of(&error)));
        }
    }
    joined
}

/// Waits for the in-flight continuations to finish, bounded by `timeout` when set. On timeout the
/// remaining continuations keep running detached; they are at-most-once side effects, so
/// abandoning them is safe.
pub(crate) async fn drain_continuations(continuations: &Tasks, timeout: Option<Duration>) {
    if continuations.pending() == 0 {
        return;
    }
    debug!(target: "ruststream::lifecycle", "draining post-settle continuations");
    if within(deadline_after(timeout), continuations.drained())
        .await
        .is_none()
    {
        warn!(
            target: "ruststream::lifecycle",
            "graceful shutdown timed out; abandoning in-flight continuations",
        );
    }
}
