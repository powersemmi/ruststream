//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. [`RustStream`](super::RustStream) owns the
//! task spawning.

use std::fmt;
use std::future::{Future, poll_fn};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use crate::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, OutgoingMessage, RetryDeclaration,
    Subscriber,
};

use super::batch::BatchHandler;
use super::context::Context;
use super::failure::{DispatchFailure, FailurePolicy, panic_reason};
use super::handler::{Handler, HandlerResult};
use super::publish::PublishContext;
#[cfg(test)]
use super::redelivery::ErasedRetryPublisher;
use super::redelivery::{DeferredRetry, ScopeDelivery};
#[cfg(feature = "testing")]
use crate::testing::coordinator::{Delivered, HarnessScope, Record, TestHooks, in_harness_scope};

/// Header carrying the framework's own retry count.
///
/// The runtime increments it on every copy of a delivery it publishes, and reads it back where the
/// transport counts nothing of its own
/// ([`IncomingMessage::redelivery_count`](crate::IncomingMessage::redelivery_count)): that is what
/// a registration's [`max_attempts`](super::RouterWith::max_attempts) cap counts there, on the
/// runtime's copy path and on a delay a broker crate honours by publishing a copy of its own
/// alike. A handler can read it too, to tell a first delivery from a redelivery.
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

/// Per-subscription publish context threaded into every delivery's [`Context`]: the
/// broker-agnostic `retry_after` fallback of this registration and the app-wide tracker for
/// post-settle continuations.
/// An `and_after` continuation is spawned onto `tasks` so a graceful shutdown drains it.
pub(crate) struct Delivery<C = ()> {
    /// The retry path of this subscription: the publisher a copy of a delivery leaves through,
    /// with the destination the mount site or the descriptor named. `None` where the broker moves
    /// the delivery itself, in which case a `NackAfter` on a transport with no native delayed
    /// redelivery has nothing left to do but requeue.
    pub(crate) retry: Option<DeferredRetry<C>>,
    /// What the mount site declared with `max_attempts(..)` and `dead_letter(..)`. Read only
    /// where the runtime is the one moving the delivery.
    pub(crate) declaration: RetryDeclaration,
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

impl<C> Delivery<C> {
    /// The context one subscription dispatches under: its own retry path over what the whole
    /// scope shares.
    pub(crate) fn for_subscription(
        scope: &ScopeDelivery,
        retry: Option<DeferredRetry<C>>,
        declaration: RetryDeclaration,
    ) -> Self {
        Self {
            retry,
            declaration,
            tasks: scope.tasks().clone(),
            #[cfg(feature = "testing")]
            hooks: Arc::clone(scope.hooks()),
            #[cfg(feature = "testing")]
            scope_id: scope.scope_id(),
        }
    }

    /// A delivery context outside any scope, for tests that drive the dispatch functions
    /// directly.
    #[cfg(test)]
    pub(crate) fn detached(retry: Option<DeferredRetry<C>>, tasks: TaskTracker) -> Self {
        Self {
            retry,
            declaration: RetryDeclaration::new(),
            tasks,
            #[cfg(feature = "testing")]
            hooks: Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            scope_id: 0,
        }
    }

    /// A delivery context publishing its retry copies through `publisher` at `destination`. For
    /// tests.
    #[cfg(test)]
    pub(crate) fn deferring_to(
        publisher: Arc<dyn ErasedRetryPublisher<C>>,
        destination: &str,
        tasks: TaskTracker,
    ) -> Self {
        Self::detached(
            Some(DeferredRetry {
                publisher,
                destination: Some(Arc::from(destination)),
            }),
            tasks,
        )
    }

    /// The same context with a declaration on it, as a mount site's `max_attempts(..)` and
    /// `dead_letter(..)` leave it. For tests.
    #[cfg(test)]
    pub(crate) fn declaring(mut self, declaration: RetryDeclaration) -> Self {
        self.declaration = declaration;
        self
    }

    /// An empty delivery context: no deferred retry, a fresh continuation tracker. For tests.
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

impl<C> fmt::Debug for Delivery<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field(
                "retry_destination",
                &self.retry.as_ref().map(|retry| &retry.destination),
            )
            .field("declaration", &self.declaration)
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
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(subscriber.stream());
        let mut cancelled = std::pin::pin!(shutdown.cancelled());
        loop {
            match turn(&shutdown, stream.as_mut(), cancelled.as_mut()).await {
                Turn::Delivery(Ok(msg)) => {
                    // Pinned rather than awaited in place: a delivery's future carries the
                    // context, the handler's own state and the settle path, and awaiting the
                    // call expression makes the loop build it on the stack and copy it into
                    // its own state on every delivery.
                    let handling = std::pin::pin!(dispatch(
                        &*handler, msg, &name, &state, &delivery, &failure
                    ));
                    handling.await;
                }
                Turn::Delivery(Err(err)) => {
                    error!(
                        target: "ruststream::dispatch",
                        error = %err,
                        "subscriber stream error",
                    );
                }
                Turn::Ended => {
                    debug!(
                        target: "ruststream::dispatch",
                        subscriber = %name,
                        "subscriber stream ended",
                    );
                    break;
                }
                Turn::Shutdown => break,
            }
        }
    })
}

/// What one turn of a dispatch loop found on its subscriber.
enum Turn<T> {
    /// The subscriber yielded a delivery, or an error reading one.
    Delivery(T),
    /// The subscriber's stream ended; the loop is done.
    Ended,
    /// Shutdown was signalled; the loop stops without touching the stream again.
    Shutdown,
}

/// Waits for whichever comes first: the next item off `stream`, or `shutdown`.
///
/// `cancelled` is the token's own wait future, built once per subscription by the caller and
/// pinned across the whole loop. A delivery therefore costs one read of the token's flag rather
/// than a future created, registered with the token's waiter list and dropped again - which is
/// what a `select!` over `cancelled()` charges on every iteration. The future is polled only
/// where the stream has nothing ready, which is the only case that has to wait for anything.
///
/// # Cancel safety
///
/// Both halves are cancel-safe: dropping this future loses no item (a `Stream` keeps its own
/// state across `poll_next`) and no wakeup (a registration with the token's waiter list outlives
/// the poll). Once `cancelled` has resolved the flag stays set - a token never un-cancels - so
/// the check at the top answers every later turn and the resolved future is never polled again.
async fn turn<St, Wait>(
    shutdown: &CancellationToken,
    mut stream: Pin<&mut St>,
    mut cancelled: Pin<&mut Wait>,
) -> Turn<St::Item>
where
    St: Stream,
    Wait: Future<Output = ()>,
{
    if shutdown.is_cancelled() {
        return Turn::Shutdown;
    }
    poll_fn(move |cx| match stream.as_mut().poll_next(cx) {
        Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
        Poll::Ready(None) => Poll::Ready(Turn::Ended),
        Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
    })
    .await
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
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
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
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
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
                        let failure = failure.clone();
                        tasks.spawn(async move {
                            dispatch(&*handler, msg, &name, &state, &delivery, &failure).await;
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
    })
}

#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
fn spawn_dispatch_lanes<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        // One sequential worker per lane, fed by a capacity-1 channel: a keyed delivery always
        // lands in the lane its key hashes to, so per-key order is preserved. In-flight cap is
        // one processing plus one queued delivery per lane.
        let mut lanes = Vec::with_capacity(workers.count);
        let mut tasks = JoinSet::new();
        for _ in 0..workers.count {
            let (tx, mut rx) = mpsc::channel::<S::Message>(1);
            let handler = Arc::clone(&handler);
            let name = Arc::clone(&name);
            let state = Arc::clone(&state);
            let delivery = Arc::clone(&delivery);
            let failure = failure.clone();
            tasks.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    dispatch(&*handler, msg, &name, &state, &delivery, &failure).await;
                }
            });
            lanes.push(tx);
        }

        let mut stream = std::pin::pin!(subscriber.stream());
        let mut cancelled = std::pin::pin!(shutdown.cancelled());
        let mut unkeyed_rotation = 0usize;
        loop {
            match turn(&shutdown, stream.as_mut(), cancelled.as_mut()).await {
                Turn::Delivery(Ok(msg)) => {
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
                Turn::Delivery(Err(err)) => {
                    error!(
                        target: "ruststream::dispatch",
                        error = %err,
                        "subscriber stream error",
                    );
                }
                Turn::Ended => {
                    debug!(
                        target: "ruststream::dispatch",
                        subscriber = %name,
                        "subscriber stream ended",
                    );
                    break;
                }
                Turn::Shutdown => break,
            }
        }
        // Closing the channels lets each lane drain its queued delivery and exit.
        drop(lanes);
        while let Some(joined) = tasks.join_next().await {
            log_worker_exit(joined);
        }
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
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
    batch_size: NonZeroUsize,
) -> JoinHandle<()>
where
    S: BatchSubscriber + Send + 'static,
    S::Message: Send + 'static,
    H: BatchHandler<S::Message, C, St> + 'static,
    C: crate::BuildBatchContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    tokio::spawn(async move {
        // The registration's own batch size, straight to the broker: whatever comes back is the
        // batch the handler sees.
        let mut stream = std::pin::pin!(subscriber.batches(batch_size));
        let mut tasks = JoinSet::new();
        // One decode buffer for the whole loop: the sequential path lends the same one to every
        // batch, so the slice a handler reads is allocated once for the subscription.
        let mut scratch = <H as BatchHandler<S::Message, C, St>>::Scratch::default();
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
                                &*handler,
                                batch,
                                &mut scratch,
                                &name,
                                &state,
                                &delivery,
                                &failure,
                            )
                            .await;
                        } else {
                            let handler = Arc::clone(&handler);
                            let name = Arc::clone(&name);
                            let state = Arc::clone(&state);
                            let delivery = Arc::clone(&delivery);
                            let failure = failure.clone();
                            tasks.spawn(async move {
                                run_pooled_batch::<_, _, C, _>(
                                    &*handler, batch, &name, &state, &delivery, &failure,
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
    })
}

async fn dispatch<H, M, C, St>(
    handler: &H,
    msg: M,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    failure: &DispatchFailure,
) where
    H: Handler<M, C, St>,
    C: crate::BuildContext<M> + Send + Sync + 'static,
    M: IncomingMessage,
    // The dispatch future is awaited inside a spawned task, so it must be `Send`: the context
    // borrows `&St` across the handler await, which requires `St: Sync`.
    St: Send + Sync,
{
    // Settling the message is what releases the harness's quiescence wait, and the post-settle
    // continuations are spawned after it; one in-flight token spanning the whole dispatch keeps a
    // `drain` from running before they exist.
    #[cfg(feature = "testing")]
    let watcher = delivery.hooks.coordinator().cloned();
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.enqueued();
    }
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
    // runs no hooks. Most deliveries register none, and those pay the branch alone: the list, its
    // scan and the drop glue of both belong to the deliveries that did register one.
    let continuations = match settle.as_ref() {
        Some(s) if ctx.has_hooks() => Some(ctx.take_hooks_for(s.outcome())),
        _ => None,
    };
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
        // Named for the same reason as the delivery's own future above: the settle path is
        // built where it is polled instead of being copied into this future's state.
        let settling = settle_outcome(msg, s.outcome(), name, delivery, C::build as fn(&M) -> C);
        settling.await;
        // Spawn the `and_after` continuation (if any) onto the tracked set so a graceful shutdown
        // drains it. At-most-once: the message is already settled, so a lost or panicking
        // continuation never redelivers it.
        if let Some(after) = s.take_after() {
            delivery.tasks.spawn(after);
        }
    }
    // Context-registered hooks run after the message is settled: at-most-once, off the delivery
    // path. They ride the same app-wide tracker as an `and_after` continuation, so one drain
    // covers both - the harness's `drain` and the shutdown's alike.
    if let Some(continuations) = continuations {
        for fut in continuations {
            delivery.tasks.spawn(fut);
        }
    }
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.consumed();
    }
}

/// Runs one batch through its handler under panic protection. The handler owns and settles the
/// batch's deliveries, so a panic there has already consumed them: the panic policy can only tear
/// the service down (`fail_fast`) or be logged and skipped. Per-element settlement is out of scope
/// (see the batch decode path for per-element decode handling). Ungated `after_settle` hooks run
/// once the batch has settled (per-element outcomes make a gated hook ill-defined on a batch).
async fn run_batch<H, M, C, St>(
    handler: &H,
    batch: Vec<M>,
    scratch: &mut H::Scratch,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    failure: &DispatchFailure,
) where
    H: BatchHandler<M, C, St>,
    M: IncomingMessage,
    C: crate::BuildBatchContext<M> + Send + Sync + 'static,
    St: Send + Sync,
{
    // A batch with no deliveries has nothing to settle and no first delivery to build a context
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
    // batch settle path record the batch it applied.
    // A panicking batch settles nothing, so its payloads are captured here (the handler owns the
    // deliveries and a panic consumes them) to record the call the settle path never reached.
    #[cfg(feature = "testing")]
    let payloads: Vec<Bytes> = batch
        .iter()
        .map(|msg| Bytes::copy_from_slice(msg.payload()))
        .collect();
    // A batch settles its own deliveries inside the handler (a panic settles them by dropping
    // them), so the last decrement lands before the batch record and the fail-fast signal below.
    // One extra in-flight token spans the whole dispatch, so a harness driving to quiescence
    // cannot return into that window.
    #[cfg(feature = "testing")]
    let watcher = delivery.hooks.coordinator().cloned();
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.enqueued();
    }
    #[cfg(feature = "testing")]
    let result = in_harness_scope(
        harness_scope(delivery),
        AssertUnwindSafe(handler.handle_batch(batch, scratch, &mut ctx)).catch_unwind(),
    )
    .await;
    #[cfg(not(feature = "testing"))]
    let result = AssertUnwindSafe(handler.handle_batch(batch, scratch, &mut ctx))
        .catch_unwind()
        .await;
    match result {
        Ok(()) => {
            // As on the single-message path: a batch that registered no hook pays the branch.
            if ctx.has_hooks() {
                for fut in ctx.take_settle_hooks() {
                    delivery.tasks.spawn(fut);
                }
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
                    deliveries: payloads
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
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.consumed();
    }
}

/// [`run_batch`] with a decode buffer of its own: the pooled path runs its batches at the same
/// time, so the loop's single buffer cannot be lent to all of them.
async fn run_pooled_batch<H, M, C, St>(
    handler: &H,
    batch: Vec<M>,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    failure: &DispatchFailure,
) where
    H: BatchHandler<M, C, St>,
    M: IncomingMessage,
    C: crate::BuildBatchContext<M> + Send + Sync + 'static,
    St: Send + Sync,
{
    let mut scratch = H::Scratch::default();
    run_batch(handler, batch, &mut scratch, name, state, delivery, failure).await;
}

/// The harness scope a delivery runs under, or `None` when no [`TestApp`](crate::testing::TestApp)
/// is driving this app.
#[cfg(feature = "testing")]
fn harness_scope<C>(delivery: &Delivery<C>) -> Option<HarnessScope> {
    delivery
        .hooks
        .coordinator()
        .cloned()
        .map(|coordinator| HarnessScope::new(coordinator, delivery.scope_id))
}

/// Settles one delivery by `outcome`, logging an ack / nack failure without propagating it.
///
/// The single place a settlement reaches the broker, single-message and batch paths alike: a
/// second one would be free to answer a [`NackAfter`](HandlerResult::NackAfter) differently, and
/// the retry path below is exactly the part that is easy to leave out.
pub(crate) async fn settle_outcome<M, C>(
    msg: M,
    outcome: HandlerResult,
    name: &str,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    let ack_result = match outcome {
        HandlerResult::Ack => msg.ack().await,
        HandlerResult::Nack { requeue: false } => msg.nack(false).await,
        HandlerResult::Nack { requeue: true } => settle_retry(msg, name, delivery, build_cx).await,
        HandlerResult::NackAfter { delay } => {
            settle_nack_after(msg, name, delay, delivery, build_cx).await
        }
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

/// Where the next copy of one delivery goes, read off the registration's declaration and the
/// count the delivery carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Redelivery<'a> {
    /// Back to the subscription itself: the attempts are not spent and no destination has taken
    /// the copies over.
    Subscription,
    /// To the declared destination.
    DeadLetter {
        /// Where the copy is published.
        destination: &'a str,
        /// Whether it goes there because the attempts are spent, rather than because the
        /// registration named a destination and no cap.
        at_cap: bool,
    },
    /// Rejected without a copy: the attempts are spent and the registration named nowhere to send
    /// it. A rejection keeps the broker's own dead-letter policy in play where one is configured.
    Reject,
}

/// How many times this message has been delivered, counting this delivery.
///
/// The broker's own count where the transport keeps one, the framework's header otherwise, never
/// both. A transport that counts its deliveries answers for all of them, the ones a delay or a
/// requeue brought round included. The header starts absent and is incremented on every copy
/// published for the delivery, so the first delivery counts as one either way.
fn attempt_of<M: IncomingMessage>(msg: &M) -> u64 {
    msg.redelivery_count()
        .unwrap_or_else(|| current_retry_count(msg.headers()) + 1)
}

/// Reads the registration's declaration against this delivery.
fn redelivery_of<'d, M: IncomingMessage>(
    msg: &M,
    declaration: &'d RetryDeclaration,
) -> Redelivery<'d> {
    match (declaration.max_attempts(), declaration.dead_letter()) {
        (Some(cap), destination) if attempt_of(msg) >= u64::from(cap.get()) => {
            destination.map_or(Redelivery::Reject, |destination| Redelivery::DeadLetter {
                destination,
                at_cap: true,
            })
        }
        (None, Some(destination)) => Redelivery::DeadLetter {
            destination,
            at_cap: false,
        },
        _ => Redelivery::Subscription,
    }
}

/// Settles an immediate retry ([`Nack { requeue: true }`](HandlerResult::Nack)).
///
/// A registration that declared neither a cap nor a destination gets the broker's own requeue,
/// which is what an immediate retry has always been. Under a declaration the outcome obeys it:
/// where the transport counts its own redeliveries the requeue stays the broker's and the count
/// is read from the delivery, and where it counts none the runtime publishes the copy itself so
/// that the framework's header carries the count forward and the cap applies to an immediate
/// retry as it does to a delayed one.
///
/// A registration whose copies the broker moves itself is never capped here, exactly as on the
/// native delayed path: the broker's requeue is the immediate retry, and the declaration reached
/// the subscription descriptor at startup for the broker to apply. Reading the cap here would
/// settle a spent delivery with a plain rejection, and on a queue that deletes a rejected
/// delivery (SQS) that loses the message instead of leaving it to the redrive policy.
///
/// # Errors
///
/// Returns the [`AckError`] from settling the original delivery.
async fn settle_retry<M, C>(
    msg: M,
    name: &str,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    if delivery.declaration.declares_nothing() {
        return msg.nack(true).await;
    }
    // The broker moves this subscription's deliveries itself: its requeue is what an immediate
    // retry is, and the declaration is the broker's to apply, so no cap is read here, as on the
    // native delayed path.
    let Some(retry) = delivery.retry.as_ref() else {
        return msg.nack(true).await;
    };
    match redelivery_of(&msg, &delivery.declaration) {
        // The broker counts its own redeliveries, so its requeue carries the count forward and
        // there is nothing for a copy to add.
        Redelivery::Subscription if msg.redelivery_count().is_some() => msg.nack(true).await,
        Redelivery::Subscription => {
            let destination = retry.destination.clone();
            publish_copy(msg, name, destination, None, retry, delivery, build_cx).await
        }
        Redelivery::DeadLetter {
            destination,
            at_cap,
        } => {
            if at_cap {
                warn_at_cap(name, attempt_of(&msg), Some(destination));
            }
            let destination = Arc::from(destination);
            publish_copy(
                msg,
                name,
                Some(destination),
                None,
                retry,
                delivery,
                build_cx,
            )
            .await
        }
        Redelivery::Reject => {
            warn_at_cap(name, attempt_of(&msg), None);
            msg.nack(false).await
        }
    }
}

/// Settles a [`NackAfter`](HandlerResult::NackAfter) outcome, choosing native delayed redelivery
/// or the copy the runtime publishes.
///
/// When the broker reports native support (`supports_nack_after`), this defers to
/// [`IncomingMessage::nack_after`] and nothing is published - unless the delivery is already at
/// the registration's cap. The cap is read first, from the one count the transport has (see
/// [`attempt_of`]): the broker's own where it keeps one, the framework's header otherwise, never
/// both. A delivery at the cap goes to the declared dead-letter destination or is rejected,
/// exactly as on the copy path.
/// Where the broker moves a spent delivery itself the cap is not read here at all: the
/// declaration is the subscription descriptor's to map onto the broker's own mechanism.
///
/// Without native support this captures the message, drops the original, and schedules a copy of it
/// after the delay, with the [`RETRY_COUNT_HEADER`] incremented. The copy goes to the address the
/// subscription's descriptor reported at startup, not to the subscription's name: the two differ
/// wherever a subscription is a resource of its own. It leaves through the registration's retry
/// slot, so the mount site's transforms and the publisher's own headers reach it as they reach any
/// slot publish; it carries bytes already, so no codec encodes it and the call adjusts no
/// per-message settings.
///
/// Once the registration's attempts are spent the copy goes to the declared dead-letter
/// destination instead, immediately, and with no destination declared the delivery is rejected.
///
/// A transport with no settlement at all ([`AckError::Unsupported`] from `nack`, as on MQTT at
/// `QoS` 0, `ZeroMQ`, or Redis pub/sub) still gets the copy: there is no original to drop and no
/// redelivery of the broker's own to fall back on, so the copy is the only form the retry can
/// take.
///
/// # Errors
///
/// Returns the [`AckError`] from settling the original when the transport does settle but this
/// settle failed. The delivery then stays with the broker, which redelivers it on its own timers,
/// and a copy on top of that would duplicate the message; the caller logs the error.
///
/// # Cancel safety
///
/// The deferred copy runs on a detached task that sleeps for `delay`. It is at-most-once over
/// that window: if the process exits (or the runtime is dropped) before the timer fires, the
/// deferred message is lost, since the original has already been dropped. Brokers that need
/// at-least-once delayed redelivery across a crash must provide native support.
async fn settle_nack_after<M, C>(
    msg: M,
    name: &str,
    delay: Duration,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    if msg.supports_nack_after() {
        // The cap is read before the delay reaches the broker: a native redelivery would otherwise
        // circle past a cap nothing in this process ever gets to apply. It is read wherever this
        // process publishes the subscription's copies, because a crate that honours the delay by
        // republishing the delivery increments the header the cap counts with where the transport
        // counts nothing; a broker that moves a spent delivery itself applies the declaration
        // itself.
        if let Some(retry) = delivery.retry.as_ref()
            && delivery.declaration.max_attempts().is_some()
        {
            match redelivery_of(&msg, &delivery.declaration) {
                Redelivery::DeadLetter { destination, .. } => {
                    warn_at_cap(name, attempt_of(&msg), Some(destination));
                    let destination = Arc::from(destination);
                    return publish_copy(
                        msg,
                        name,
                        Some(destination),
                        None,
                        retry,
                        delivery,
                        build_cx,
                    )
                    .await;
                }
                Redelivery::Reject => {
                    warn_at_cap(name, attempt_of(&msg), None);
                    return msg.nack(false).await;
                }
                Redelivery::Subscription => {}
            }
        }
        return msg.nack_after(delay).await;
    }

    let Some(retry) = delivery.retry.as_ref() else {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            "retry_after on a broker with neither native delayed redelivery nor a copy path of \
             its own; requeuing immediately (the delay is dropped)",
        );
        return msg.nack(true).await;
    };

    match redelivery_of(&msg, &delivery.declaration) {
        Redelivery::Subscription => {
            let destination = retry.destination.clone();
            publish_copy(
                msg,
                name,
                destination,
                Some(delay),
                retry,
                delivery,
                build_cx,
            )
            .await
        }
        Redelivery::DeadLetter {
            destination,
            at_cap,
        } => {
            if at_cap {
                warn_at_cap(name, attempt_of(&msg), Some(destination));
            }
            // A delivery that has run out of attempts is not retried, so the delay it asked for
            // does not apply to the copy that carries it away.
            let destination = Arc::from(destination);
            publish_copy(
                msg,
                name,
                Some(destination),
                None,
                retry,
                delivery,
                build_cx,
            )
            .await
        }
        Redelivery::Reject => {
            warn_at_cap(name, attempt_of(&msg), None);
            msg.nack(false).await
        }
    }
}

/// Says once per exhausted delivery where it went, because that is the point at which a message
/// stops coming back and an operator has to know.
fn warn_at_cap(name: &str, attempt: u64, destination: Option<&str>) {
    if let Some(destination) = destination {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            attempt,
            dead_letter = %destination,
            "retry attempts exhausted; publishing the delivery to the dead-letter destination",
        );
    } else {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            attempt,
            "retry attempts exhausted and no dead-letter destination is declared; rejecting the \
             delivery",
        );
    }
}

/// Drops the original delivery and sends a copy of it to `destination`, now or after `delay`.
///
/// `destination` is `None` only where the registration's transforms name one per delivery; the
/// copy then starts at the subscription's own name, which is what the transforms read as the
/// delivery's channel, and one of them renames it.
///
/// # Errors
///
/// Returns the [`AckError`] from dropping the original where that failed and the transport does
/// settle: the delivery then stays with the broker, and a copy on top of it would duplicate the
/// message.
async fn publish_copy<M, C>(
    msg: M,
    name: &str,
    destination: Option<Arc<str>>,
    delay: Option<Duration>,
    retry: &DeferredRetry<C>,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    let publisher = Arc::clone(&retry.publisher);
    let subscription: Arc<str> = Arc::from(name);
    let target = destination.unwrap_or_else(|| Arc::clone(&subscription));

    // Settling consumes the message, so capture everything the copy needs first: its bytes, the
    // headers the handler saw, and the broker's own per-delivery context, which is what a
    // transform on this position reads.
    let payload = Bytes::copy_from_slice(msg.payload());
    let delivered = msg.headers().clone();
    let mut headers = delivered.clone();
    let next_count = current_retry_count(&headers) + 1;
    headers.insert(RETRY_COUNT_HEADER, next_count.to_string());
    let context = build_cx(&msg);

    // Drop the original so the broker does not also redeliver it; the copy carries the retry
    // forward. A transport with no settlement at all has nothing to drop and no redelivery of its
    // own, so there the copy is the only way the message survives: aborting on that error would
    // lose it. Any other settle failure leaves the delivery with the broker, which will redeliver
    // it on its own timers, so a copy on top would duplicate it.
    match msg.nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(err) => return Err(err),
    }

    let republish = async move {
        let cx = PublishContext::new(&subscription, &delivered, &context);
        let copy = OutgoingMessage::new(target.as_ref(), payload.as_ref()).with_headers(headers);
        if let Err(err) = publisher.publish_copy(copy, &cx).await {
            warn!(
                target: "ruststream::dispatch",
                subscription = %subscription,
                destination = %target,
                error = %err,
                "publishing the retry copy failed; message lost",
            );
        }
    };

    let Some(delay) = delay else {
        // An immediate copy is awaited on the dispatch path: there is no timer to wait for, and a
        // detached task would let the loop pull the next delivery before this one is back.
        republish.await;
        return Ok(());
    };

    // Under the harness the copy is scheduled through the coordinator, the way a broker schedules
    // its native delayed redelivery: `TestApp::advance` then fires it and counts it before it
    // drives the reaction, so a test sees the copy rather than a race.
    #[cfg(feature = "testing")]
    if let Some(coordinator) = delivery.hooks.coordinator() {
        coordinator.schedule_redelivery_future(delay, republish);
        return Ok(());
    }
    #[cfg(not(feature = "testing"))]
    let _ = delivery;

    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        republish.await;
    });
    Ok(())
}

#[cfg(all(test, feature = "memory"))]
mod tests;
