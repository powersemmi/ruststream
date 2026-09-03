//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. [`RustStream`](super::RustStream) owns the
//! task spawning.

use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use crate::{AckError, BatchSubscriber, HeaderMap, IncomingMessage, Subscriber};

use super::batch::BatchHandler;
use super::context::Context;
use super::failure::{DispatchFailure, FailurePolicy, panic_reason};
use super::handler::{Handler, HandlerResult};
use super::publish::raw_of;
use super::publisher_registry::{ErasedPublisher, ErasedSink};
#[cfg(feature = "testing")]
use crate::testing::coordinator::{Delivered, HarnessScope, Record, TestHooks, in_harness_scope};

/// Header carrying the framework's deferred-republish retry count.
///
/// The broker-agnostic `retry_after` fallback increments this on each deferred re-publish, so a
/// handler can read it to cap its own retries (a poison-message guard). It counts only the
/// framework's own deferred republishes, not a broker's native redeliveries.
///
/// # Examples
///
/// ```
/// use ruststream::HeaderMap;
/// use ruststream::runtime::RETRY_COUNT_HEADER;
///
/// fn over_limit(headers: &HeaderMap, limit: u64) -> bool {
///     let count: u64 = headers.get_str(RETRY_COUNT_HEADER).and_then(|v| v.parse().ok()).unwrap_or(0);
///     count >= limit
/// }
///
/// let mut headers = HeaderMap::new();
/// headers.insert(RETRY_COUNT_HEADER, "3");
/// assert!(over_limit(&headers, 3));
/// ```
pub const RETRY_COUNT_HEADER: &str = "x-ruststream-retry-count";

/// Parses the current [`RETRY_COUNT_HEADER`] value, defaulting to zero when absent or malformed.
fn current_retry_count(headers: &HeaderMap) -> u64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Concurrency policy for one subscriber's dispatch loop, declared with the `workers(..)` macro
/// argument (or [`Workers::sequential`] by default).
///
/// - `workers(n)`: up to `n` deliveries of the subscriber processed concurrently, each in its
///   own task on the multi-thread runtime. Back-pressure holds: the stream is not polled while
///   `n` deliveries are in flight. Global processing order is lost by design.
/// - `workers(n, by_key)`: `n` lanes; a delivery goes to the lane picked by hashing its
///   [`partition_key`](crate::IncomingMessage::partition_key), and each lane is sequential, so
///   per-key ordering is preserved. Messages without a key rotate over the lanes.
///
/// The default is sequential dispatch (`workers(1)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Workers {
    count: usize,
    by_key: bool,
}

impl Workers {
    /// Sequential dispatch: one delivery at a time, in stream order. The default.
    #[must_use]
    pub const fn sequential() -> Self {
        Self {
            count: 1,
            by_key: false,
        }
    }

    /// A pool of up to `count` concurrent deliveries. One behaves like sequential dispatch.
    #[must_use]
    pub const fn pool(count: NonZeroUsize) -> Self {
        Self {
            count: count.get(),
            by_key: false,
        }
    }

    /// `count` sequential lanes keyed by the message
    /// [`partition_key`](crate::IncomingMessage::partition_key): per-key ordering is preserved.
    /// One lane behaves like sequential dispatch.
    #[must_use]
    pub const fn keyed(count: NonZeroUsize) -> Self {
        Self {
            count: count.get(),
            by_key: true,
        }
    }

    /// One worker is indistinguishable from the sequential loop.
    pub(crate) const fn is_sequential(&self) -> bool {
        self.count <= 1
    }
}

impl Default for Workers {
    fn default() -> Self {
        Self::sequential()
    }
}

/// Per-scope publish context threaded into every delivery's [`Context`]: the broker-agnostic
/// `retry_after` fallback publisher and the app-wide tracker for post-settle continuations. An
/// `and_after` continuation is spawned onto `tasks` so a graceful shutdown drains it.
pub(crate) struct Delivery {
    /// Publisher used by the broker-agnostic `retry_after` fallback to re-publish a message to its
    /// own source subject after the delay. `None` when the scope did not opt in, in which case a
    /// `NackAfter` on a non-native broker degrades to an immediate requeue (with a warning).
    pub(crate) retry_publisher: Option<Arc<dyn ErasedPublisher>>,
    /// Per-scope task tracker for post-settle `and_after` continuations. The
    /// dispatcher spawns each element's continuation onto it after settling, so a graceful
    /// shutdown drains them.
    pub(crate) tasks: TaskTracker,
    /// The harness's recording-and-quiescence hooks for this scope. Empty (uninstalled) outside a
    /// [`TestApp`](crate::testing::TestApp) run, so the per-delivery read is a single atomic load.
    #[cfg(feature = "testing")]
    pub(crate) hooks: Arc<TestHooks>,
    /// This broker's registration index, used to scope recorded deliveries per broker.
    #[cfg(feature = "testing")]
    pub(crate) scope_id: usize,
}

impl Delivery {
    /// A delivery context with no test instrumentation (production, and tests that do not drive the
    /// harness). With the `testing` feature, `collect_scope` uses [`instrumented`](Self::instrumented)
    /// instead, so this is reachable only in the non-testing build or from unit tests.
    #[cfg(any(not(feature = "testing"), test))]
    pub(crate) fn detached(
        retry_publisher: Option<Arc<dyn ErasedPublisher>>,
        tasks: TaskTracker,
    ) -> Self {
        Self {
            retry_publisher,
            tasks,
            #[cfg(feature = "testing")]
            hooks: Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            scope_id: 0,
        }
    }

    /// A delivery context carrying the harness hooks and this broker's scope id.
    #[cfg(feature = "testing")]
    pub(crate) fn instrumented(
        retry_publisher: Option<Arc<dyn ErasedPublisher>>,
        tasks: TaskTracker,
        hooks: Arc<TestHooks>,
        scope_id: usize,
    ) -> Self {
        Self {
            retry_publisher,
            tasks,
            hooks,
            scope_id,
        }
    }

    /// An empty delivery context: no retry publisher, a fresh continuation tracker. For tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::with_tasks(TaskTracker::new())
    }

    /// An empty delivery context carrying a caller-owned continuation tracker, so a test can
    /// observe the post-settle continuations spawned through it.
    #[cfg(test)]
    pub(crate) fn with_tasks(tasks: TaskTracker) -> Self {
        Self::detached(None, tasks)
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("retry_publisher", &self.retry_publisher.is_some())
            .field("pending_continuations", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates. Each delivery is given a [`Context`] built from `name`, the message headers,
/// shared `state`, and the `delivery` publish context.
pub(crate) fn spawn_dispatch<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery>,
    failure: DispatchFailure,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let hooks = TaskTracker::new();
        let mut stream = std::pin::pin!(subscriber.stream());
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                next = stream.next() => match next {
                    Some(Ok(msg)) => {
                        dispatch(&*handler, msg, &name, &state, &delivery, &hooks, &failure).await;
                    }
                    Some(Err(err)) => {
                        error!(
                            target: "ruststream::dispatch",
                            error = %err,
                            "subscriber stream error",
                        );
                    }
                    None => {
                        debug!(
                            target: "ruststream::dispatch",
                            subscriber = %name,
                            "subscriber stream ended",
                        );
                        break;
                    }
                }
            }
        }
        drain_hooks(hooks).await;
    })
}

/// Closes a hook tracker to new spawns and waits for the in-flight post-settle continuations to
/// finish. Called once the dispatch loop exits; bounded from the outside by the app's
/// `shutdown_timeout`, which aborts the whole dispatch task (and these hooks with it) on timeout.
async fn drain_hooks(hooks: TaskTracker) {
    hooks.close();
    hooks.wait().await;
}

/// Spawns a task that drives `subscriber` through `handler` with a bounded worker pool: up to
/// `workers.count` deliveries in flight, each handled (and settled) in its own task. With
/// `by_key`, the pool becomes per-key sequential lanes instead.
///
/// Sequential policies delegate to [`spawn_dispatch`]. On shutdown the stream stops being
/// polled and in-flight workers drain; if the app's `shutdown_timeout` aborts this task, the
/// owned worker tasks abort with it.
// The parts are independent and each spawn site passes its own; bundling them into a struct
// would hide that.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_dispatch_workers<S, H, C, St>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + 'static,
    St: Send + Sync + 'static,
{
    if workers.is_sequential() {
        return spawn_dispatch(
            subscriber, handler, shutdown, name, state, delivery, failure,
        );
    }
    if workers.by_key {
        spawn_dispatch_lanes(
            subscriber, handler, shutdown, name, state, delivery, failure, workers,
        )
    } else {
        spawn_dispatch_pool(
            subscriber, handler, shutdown, name, state, delivery, failure, workers,
        )
    }
}

#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
fn spawn_dispatch_pool<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let hooks = TaskTracker::new();
        let mut stream = std::pin::pin!(subscriber.stream());
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                // The pool is full: reap a finished worker before polling for more.
                Some(joined) = tasks.join_next(), if tasks.len() >= workers.count => {
                    log_worker_exit(joined);
                }
                next = stream.next(), if tasks.len() < workers.count => match next {
                    Some(Ok(msg)) => {
                        let handler = Arc::clone(&handler);
                        let name = Arc::clone(&name);
                        let state = Arc::clone(&state);
                        let delivery = Arc::clone(&delivery);
                        let hooks = hooks.clone();
                        let failure = failure.clone();
                        tasks.spawn(async move {
                            dispatch(&*handler, msg, &name, &state, &delivery, &hooks, &failure)
                                .await;
                        });
                    }
                    Some(Err(err)) => {
                        error!(
                            target: "ruststream::dispatch",
                            error = %err,
                            "subscriber stream error",
                        );
                    }
                    None => {
                        debug!(
                            target: "ruststream::dispatch",
                            subscriber = %name,
                            "subscriber stream ended",
                        );
                        break;
                    }
                }
            }
        }
        while let Some(joined) = tasks.join_next().await {
            log_worker_exit(joined);
        }
        drain_hooks(hooks).await;
    })
}

#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
fn spawn_dispatch_lanes<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        // One sequential worker per lane, fed by a capacity-1 channel: a keyed delivery always
        // lands in the lane its key hashes to, so per-key order is preserved. In-flight cap is
        // one processing plus one queued delivery per lane.
        let hooks = TaskTracker::new();
        let mut lanes = Vec::with_capacity(workers.count);
        let mut tasks = JoinSet::new();
        for _ in 0..workers.count {
            let (tx, mut rx) = mpsc::channel::<S::Message>(1);
            let handler = Arc::clone(&handler);
            let name = Arc::clone(&name);
            let state = Arc::clone(&state);
            let delivery = Arc::clone(&delivery);
            let hooks = hooks.clone();
            let failure = failure.clone();
            tasks.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    dispatch(&*handler, msg, &name, &state, &delivery, &hooks, &failure).await;
                }
            });
            lanes.push(tx);
        }

        let mut stream = std::pin::pin!(subscriber.stream());
        let mut unkeyed_rotation = 0usize;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                next = stream.next() => match next {
                    Some(Ok(msg)) => {
                        // No key: any lane will do; rotate to spread the load.
                        let lane = msg.partition_key().map_or_else(
                            || {
                                unkeyed_rotation = (unkeyed_rotation + 1) % workers.count;
                                unkeyed_rotation
                            },
                            |key| lane_of(key, workers.count),
                        );
                        if lanes[lane].send(msg).await.is_err() {
                            // A lane only disappears if its task panicked; stop pulling rather
                            // than silently dropping deliveries for that key range.
                            error!(
                                target: "ruststream::dispatch",
                                subscriber = %name,
                                lane,
                                "worker lane terminated; stopping dispatch",
                            );
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        error!(
                            target: "ruststream::dispatch",
                            error = %err,
                            "subscriber stream error",
                        );
                    }
                    None => {
                        debug!(
                            target: "ruststream::dispatch",
                            subscriber = %name,
                            "subscriber stream ended",
                        );
                        break;
                    }
                }
            }
        }
        // Closing the channels lets each lane drain its queued delivery and exit.
        drop(lanes);
        while let Some(joined) = tasks.join_next().await {
            log_worker_exit(joined);
        }
        drain_hooks(hooks).await;
    })
}

fn lane_of(key: &[u8], lanes: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    // The modulo keeps the value below `lanes`, which is a usize.
    #[allow(clippy::cast_possible_truncation)]
    {
        (hasher.finish() % lanes as u64) as usize
    }
}

fn log_worker_exit(joined: Result<(), tokio::task::JoinError>) {
    if let Err(err) = joined {
        error!(target: "ruststream::dispatch", error = %err, "worker task failed");
    }
}

/// Spawns a task that drives `subscriber` through a batch `handler`, one
/// [`BatchSubscriber::batches`] item per invocation, until `shutdown` is triggered or the stream
/// terminates. The handler owns the batch's deliveries and settles each of them.
///
/// With a non-sequential `workers` policy, up to `workers.count` batches are in flight at once,
/// each in its own task; keyed lanes do not apply at batch granularity (the macro rejects
/// `by_key` on batch forms), so a keyed policy degrades to the plain pool.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
pub(crate) fn spawn_batch_dispatch<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: BatchSubscriber + Send + 'static,
    S::Message: Send + 'static,
    H: BatchHandler<S::Message, C, St> + 'static,
    C: crate::BuildBatchContext<S::Message> + Send + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let hooks = TaskTracker::new();
        let mut stream = std::pin::pin!(subscriber.batches());
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                // The pool is full: reap a finished worker before polling for more.
                Some(joined) = tasks.join_next(), if tasks.len() >= workers.count => {
                    log_worker_exit(joined);
                }
                next = stream.next(), if tasks.len() < workers.count => match next {
                    Some(Ok(batch)) => {
                        let batch: Vec<S::Message> = batch.into_iter().collect();
                        if workers.is_sequential() {
                            // Turbofish: the adapter handlers are generic over the batch
                            // context, so the spawn's own parameter names it.
                            run_batch::<_, _, C, _>(
                                &*handler, batch, &name, &state, &delivery, &hooks, &failure,
                            )
                            .await;
                        } else {
                            let handler = Arc::clone(&handler);
                            let name = Arc::clone(&name);
                            let state = Arc::clone(&state);
                            let delivery = Arc::clone(&delivery);
                            let hooks = hooks.clone();
                            let failure = failure.clone();
                            tasks.spawn(async move {
                                run_batch::<_, _, C, _>(
                                    &*handler, batch, &name, &state, &delivery, &hooks, &failure,
                                )
                                .await;
                            });
                        }
                    }
                    Some(Err(err)) => {
                        error!(
                            target: "ruststream::dispatch",
                            error = %err,
                            "subscriber stream error",
                        );
                    }
                    None => {
                        debug!(
                            target: "ruststream::dispatch",
                            subscriber = %name,
                            "subscriber stream ended",
                        );
                        break;
                    }
                }
            }
        }
        while let Some(joined) = tasks.join_next().await {
            log_worker_exit(joined);
        }
        drain_hooks(hooks).await;
    })
}

#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
async fn dispatch<H, M, C, St>(
    handler: &H,
    msg: M,
    name: &str,
    state: &St,
    delivery: &Delivery,
    hooks: &TaskTracker,
    failure: &DispatchFailure,
) where
    H: Handler<M, C, St>,
    C: crate::BuildContext<M>,
    M: IncomingMessage,
    // The dispatch future is awaited inside a spawned task, so it must be `Send`: the context
    // borrows `&St` across the handler await, which requires `St: Sync`.
    St: Send + Sync,
{
    // Build the broker's typed per-delivery context from the message, then attach the fail-fast
    // handle.
    let cx = C::build(&msg);
    let mut ctx = Context::new(name, msg.headers(), state, cx, delivery)
        .with_failfast(&failure.shutdown)
        .with_decode_policy(failure.policies.decode);
    // Catch a panicking handler so it cannot silently kill the dispatch loop (which would stop the
    // subscriber consuming) or leave the message unsettled. AssertUnwindSafe is required because
    // the future borrows `&mut ctx`; that state is discarded with the failed delivery.
    // Under the harness, the invocation runs in a task-local slot scope so publishes made
    // through injected `Out` publishers are attributed to their slot.
    #[cfg(feature = "testing")]
    let result = in_harness_scope(
        harness_scope(delivery),
        AssertUnwindSafe(handler.handle(&msg, &mut ctx)).catch_unwind(),
    )
    .await;
    #[cfg(not(feature = "testing"))]
    let result = AssertUnwindSafe(handler.handle(&msg, &mut ctx))
        .catch_unwind()
        .await;
    #[cfg(feature = "testing")]
    let panicked = result.is_err();
    // Resolve into a `HandlerOutcome` regardless of whether the handler panicked. `None` means a fail-fast
    // panic tore the service down and left the message unsettled (a broker with redelivery hands it
    // back after the restart).
    let settle = match result {
        Ok(s) => Some(s),
        Err(payload) => {
            let reason = panic_reason(payload.as_ref());
            error!(
                target: "ruststream::dispatch",
                subscription = %name,
                panic = %reason,
                "handler panicked",
            );
            match failure.policies.panic {
                FailurePolicy::FailFast => {
                    failure
                        .shutdown
                        .signal(name, &format!("handler panicked: {reason}"));
                    None
                }
                other => Some(
                    other
                        .settlement()
                        .map_or_else(super::handler::HandlerOutcome::drop, Into::into),
                ),
            }
        }
    };
    // Drain the matching post-settle hooks BEFORE settling: `ctx` borrows `msg`'s headers, and
    // settling consumes `msg`. The drained futures own their captures. A fail-fast (no settlement)
    // runs no hooks.
    let continuations = settle
        .as_ref()
        .map_or_else(Vec::new, |s| ctx.take_hooks_for(s.outcome()));
    // The harness records what the handler saw and how it settled, BEFORE settling the message: the
    // matching decrement runs in the broker message's `Drop` (during `settle_outcome`, or at the end
    // of this function on the fail-fast path), so the record is in place by the time `drive` wakes.
    // Captured here because `settle_outcome` consumes `msg` and `drop(ctx)` clears the decode flag.
    #[cfg(feature = "testing")]
    if let Some(coordinator) = delivery.hooks.coordinator() {
        coordinator.record(Record {
            scope_id: delivery.scope_id,
            name: name.to_owned(),
            deliveries: vec![Delivered {
                raw: Bytes::copy_from_slice(msg.payload()),
                settle: settle.as_ref().map(super::handler::HandlerOutcome::outcome),
            }],
            panicked,
            decode_failed: ctx.took_decode_failed(),
        });
    }
    drop(ctx);
    if let Some(mut s) = settle {
        settle_outcome(msg, s.outcome(), name, delivery).await;
        // Spawn the `and_after` continuation (if any) onto the tracked set so a graceful shutdown
        // drains it. At-most-once: the message is already settled, so a lost or panicking
        // continuation never redelivers it.
        if let Some(after) = s.take_after() {
            delivery.tasks.spawn(after);
        }
    }
    // Context-registered hooks run after the message is settled: at-most-once, off the delivery
    // path. Tracked so a graceful shutdown drains them.
    for fut in continuations {
        hooks.spawn(fut);
    }
}

/// Runs one batch through its handler under panic protection. The handler owns and settles the
/// batch's deliveries, so a panic there has already consumed them: the panic policy can only tear
/// the service down (`fail_fast`) or be logged and skipped. Per-element settlement is out of scope
/// (see the batch decode path for per-element decode handling). Ungated `after_settle` hooks run
/// once the batch has settled (per-element outcomes make a gated hook ill-defined on a batch).
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
async fn run_batch<H, M, C, St>(
    handler: &H,
    batch: Vec<M>,
    name: &str,
    state: &St,
    delivery: &Delivery,
    hooks: &TaskTracker,
    failure: &DispatchFailure,
) where
    H: BatchHandler<M, C, St>,
    M: IncomingMessage,
    C: crate::BuildBatchContext<M> + Send,
    St: Send + Sync,
{
    // A page with no deliveries has nothing to settle and no first delivery to build a context
    // from; nothing to do.
    let Some(first) = batch.first() else { return };
    let empty = HeaderMap::new();
    // A batch spans many deliveries, so its context carries only subscription-scoped data,
    // built from the first delivery; the shared app state is threaded the same way as on the
    // single-message path.
    let cx = C::build(first);
    let mut ctx = Context::new(name, &empty, state, cx, delivery)
        .with_failfast(&failure.shutdown)
        .with_decode_policy(failure.policies.decode);
    // See `dispatch`: the harness scope attributes `Out` publishes to their slot, and lets the
    // batch settle path record the page it applied.
    // A panicking page settles nothing, so its payloads are captured here (the handler owns the
    // deliveries and a panic consumes them) to record the call the settle path never reached.
    #[cfg(feature = "testing")]
    let page: Vec<Bytes> = batch
        .iter()
        .map(|msg| Bytes::copy_from_slice(msg.payload()))
        .collect();
    #[cfg(feature = "testing")]
    let result = in_harness_scope(
        harness_scope(delivery),
        AssertUnwindSafe(handler.handle_batch(batch, &mut ctx)).catch_unwind(),
    )
    .await;
    #[cfg(not(feature = "testing"))]
    let result = AssertUnwindSafe(handler.handle_batch(batch, &mut ctx))
        .catch_unwind()
        .await;
    match result {
        Ok(()) => {
            for fut in ctx.take_settle_hooks() {
                hooks.spawn(fut);
            }
        }
        Err(payload) => {
            let reason = panic_reason(payload.as_ref());
            error!(
                target: "ruststream::dispatch",
                subscription = %name,
                panic = %reason,
                "batch handler panicked",
            );
            #[cfg(feature = "testing")]
            if let Some(coordinator) = delivery.hooks.coordinator() {
                coordinator.record(Record {
                    scope_id: delivery.scope_id,
                    name: name.to_owned(),
                    deliveries: page
                        .into_iter()
                        .map(|raw| Delivered { raw, settle: None })
                        .collect(),
                    panicked: true,
                    decode_failed: false,
                });
            }
            if failure.policies.panic == FailurePolicy::FailFast {
                failure
                    .shutdown
                    .signal(name, &format!("batch handler panicked: {reason}"));
            }
        }
    }
}

/// The harness scope a delivery runs under, or `None` when no [`TestApp`](crate::testing::TestApp)
/// is driving this app.
#[cfg(feature = "testing")]
fn harness_scope(delivery: &Delivery) -> Option<HarnessScope> {
    delivery
        .hooks
        .coordinator()
        .cloned()
        .map(|coordinator| HarnessScope::new(coordinator, delivery.scope_id))
}

/// Settles one delivery by `outcome`, logging an ack / nack failure without propagating it.
async fn settle_outcome<M: IncomingMessage>(
    msg: M,
    outcome: HandlerResult,
    name: &str,
    delivery: &Delivery,
) {
    let ack_result = match outcome {
        HandlerResult::Ack => msg.ack().await,
        HandlerResult::Nack { requeue } => msg.nack(requeue).await,
        HandlerResult::NackAfter { delay } => settle_nack_after(msg, name, delay, delivery).await,
    };
    if let Err(err) = ack_result {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            error = %err,
            "ack / nack failed",
        );
    }
}

/// Settles a [`NackAfter`](HandlerResult::NackAfter) outcome, choosing native delayed redelivery
/// or the broker-agnostic fallback.
///
/// When the broker reports native support (`supports_nack_after`), this defers to
/// [`IncomingMessage::nack_after`]. Otherwise it captures the message, drops the original, and
/// schedules a deferred re-publish of the captured copy to its source subject with the
/// [`RETRY_COUNT_HEADER`] incremented. With no `retry_publisher` configured on the scope, it falls
/// back to an immediate requeue and warns.
///
/// # Cancel safety
///
/// The deferred re-publish runs on a detached task that sleeps for `delay`. It is at-most-once over
/// that window: if the process exits (or the runtime is dropped) before the timer fires, the
/// deferred message is lost, since the original has already been dropped. Brokers that need
/// at-least-once delayed redelivery across a crash must provide native support.
async fn settle_nack_after<M>(
    msg: M,
    name: &str,
    delay: Duration,
    delivery: &Delivery,
) -> Result<(), AckError>
where
    M: IncomingMessage,
{
    if msg.supports_nack_after() {
        return msg.nack_after(delay).await;
    }

    let Some(publisher) = delivery.retry_publisher.clone() else {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            "retry_after on a broker without native delayed redelivery and no retry publisher \
             configured; requeuing immediately (the delay is dropped)",
        );
        return msg.nack(true).await;
    };

    // nack_after consumes self, so capture everything needed for the re-publish first.
    let payload = Bytes::copy_from_slice(msg.payload());
    let mut headers = msg.headers().clone();
    let next_count = current_retry_count(&headers) + 1;
    headers.insert(RETRY_COUNT_HEADER, next_count.to_string());
    let subject = name.to_owned();

    // Drop the original so the broker does not also redeliver it; the deferred copy carries the
    // retry forward.
    msg.nack(false).await?;

    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let deferred = raw_of(ErasedSink(publisher.as_ref()), &payload)
            .with_headers(headers)
            .to(subject.as_str());
        if let Err(err) = deferred.publish().await {
            warn!(
                target: "ruststream::dispatch",
                subscription = %subject,
                error = %err,
                "deferred retry_after re-publish failed; message lost",
            );
        }
    });
    Ok(())
}

#[cfg(all(test, feature = "memory"))]
mod tests;
