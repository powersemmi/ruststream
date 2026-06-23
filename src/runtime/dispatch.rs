//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. Lifted out of the former `Router` so
//! [`RustStream`](super::RustStream) can own task spawning directly.

use std::hash::{DefaultHasher, Hash, Hasher};
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

use crate::{AckError, BatchSubscriber, Headers, IncomingMessage, Subscriber};

use super::batch::BatchHandler;
use super::context::Context;
use super::failure::{DispatchFailure, FailurePolicy, panic_reason};
use super::handler::{Handler, HandlerResult};
use super::publisher_registry::ErasedPublisher;

/// Header carrying the framework's deferred-republish retry count.
///
/// The broker-agnostic `retry_after` fallback increments this on each deferred re-publish, so a
/// handler can read it to cap its own retries (a poison-message guard). It counts only the
/// framework's own deferred republishes, not a broker's native redeliveries.
///
/// # Examples
///
/// ```
/// use ruststream::Headers;
/// use ruststream::runtime::RETRY_COUNT_HEADER;
///
/// fn over_limit(headers: &Headers, limit: u64) -> bool {
///     let count: u64 = headers.get_str(RETRY_COUNT_HEADER).and_then(|v| v.parse().ok()).unwrap_or(0);
///     count >= limit
/// }
///
/// let mut headers = Headers::new();
/// headers.insert(RETRY_COUNT_HEADER, "3");
/// assert!(over_limit(&headers, 3));
/// ```
pub const RETRY_COUNT_HEADER: &str = "x-ruststream-retry-count";

/// Parses the current [`RETRY_COUNT_HEADER`] value, defaulting to zero when absent or malformed.
fn current_retry_count(headers: &Headers) -> u64 {
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
/// The default is sequential dispatch (`workers(1)`), the pre-pool behaviour.
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

    /// A pool of up to `count` concurrent deliveries. Zero behaves like one (sequential).
    #[must_use]
    pub const fn pool(count: usize) -> Self {
        Self {
            count: if count == 0 { 1 } else { count },
            by_key: false,
        }
    }

    /// `count` sequential lanes keyed by the message
    /// [`partition_key`](crate::IncomingMessage::partition_key): per-key ordering is preserved.
    /// Zero or one lane behaves like sequential dispatch.
    #[must_use]
    pub const fn keyed(count: usize) -> Self {
        Self {
            count: if count == 0 { 1 } else { count },
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
    /// Per-scope task tracker for post-settle [`HandlerResult::and_after`] continuations. The
    /// dispatcher spawns each element's continuation onto it after settling, so a graceful
    /// shutdown drains them.
    pub(crate) tasks: TaskTracker,
}

impl Delivery {
    /// An empty delivery context: no retry publisher, a fresh continuation tracker. For tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::with_tasks(TaskTracker::new())
    }

    /// An empty delivery context carrying a caller-owned continuation tracker, so a test can
    /// observe the post-settle continuations spawned through it.
    #[cfg(test)]
    pub(crate) fn with_tasks(tasks: TaskTracker) -> Self {
        Self {
            retry_publisher: None,
            tasks,
        }
    }
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
// The dispatch loop is wired from many independent runtime parts (subscriber, handler, shutdown,
// identity, state, publish context, failure policy, worker policy); bundling them only to satisfy
// the arg-count lint would hide what each spawn site passes.
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
pub(crate) fn spawn_batch_dispatch<S, H, St>(
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
    H: BatchHandler<S::Message, St> + 'static,
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
                            run_batch(&*handler, batch, &name, &state, &delivery, &hooks, &failure)
                                .await;
                        } else {
                            let handler = Arc::clone(&handler);
                            let name = Arc::clone(&name);
                            let state = Arc::clone(&state);
                            let delivery = Arc::clone(&delivery);
                            let hooks = hooks.clone();
                            let failure = failure.clone();
                            tasks.spawn(async move {
                                run_batch(
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
    let mut ctx =
        Context::new(name, msg.headers(), state, cx, delivery).with_failfast(&failure.shutdown);
    // Catch a panicking handler so it cannot silently kill the dispatch loop (which would stop the
    // subscriber consuming) or leave the message unsettled. AssertUnwindSafe is required because
    // the future borrows `&mut ctx`; that state is discarded with the failed delivery.
    let result = AssertUnwindSafe(handler.handle(&msg, &mut ctx))
        .catch_unwind()
        .await;
    // Resolve into a `Settle` regardless of whether the handler panicked. `None` means a fail-fast
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
                        .unwrap_or_else(HandlerResult::drop)
                        .into(),
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
async fn run_batch<H, M, St>(
    handler: &H,
    batch: Vec<M>,
    name: &str,
    state: &St,
    delivery: &Delivery,
    hooks: &TaskTracker,
    failure: &DispatchFailure,
) where
    H: BatchHandler<M, St>,
    M: IncomingMessage,
    St: Send + Sync,
{
    let empty = Headers::new();
    // A batch has no single broker message, so its per-delivery context is unit (`C = ()`); the
    // shared app state is threaded the same way as on the single-message path.
    let mut ctx = Context::new(name, &empty, state, (), delivery).with_failfast(&failure.shutdown);
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
            if failure.policies.panic == FailurePolicy::FailFast {
                failure
                    .shutdown
                    .signal(name, &format!("batch handler panicked: {reason}"));
            }
        }
    }
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
/// back to an immediate requeue (the legacy behavior) and warns.
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
        if let Err(err) = publisher
            .publish_message(&subject, &payload, &headers)
            .await
        {
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
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };

    use futures::StreamExt;

    use super::*;
    use crate::memory::MemoryBroker;
    use crate::{AckError, Headers, IncomingMessage, OutgoingMessage, Publisher};

    /// A delivery without native delayed redelivery: `supports_nack_after` stays at the trait
    /// default (`false`), and the default `nack_after` would error. It records how it was settled
    /// so a test can assert the fallback dropped it rather than calling `nack(true)`.
    struct PlainMessage {
        payload: Bytes,
        headers: Headers,
        // 0 = unset, 1 = nack(false) (dropped), 2 = nack(true) (requeued).
        settled: Arc<AtomicU8>,
    }

    impl IncomingMessage for PlainMessage {
        fn payload(&self) -> &[u8] {
            &self.payload
        }

        fn headers(&self) -> &Headers {
            &self.headers
        }

        async fn ack(self) -> Result<(), AckError> {
            Ok(())
        }

        async fn nack(self, requeue: bool) -> Result<(), AckError> {
            self.settled
                .store(if requeue { 2 } else { 1 }, Ordering::SeqCst);
            Ok(())
        }
    }

    fn plain(name_headers: &[(&str, &str)], settled: &Arc<AtomicU8>) -> PlainMessage {
        let mut headers = Headers::new();
        for (k, v) in name_headers {
            headers.insert((*k).to_owned(), Bytes::copy_from_slice(v.as_bytes()));
        }
        PlainMessage {
            payload: Bytes::from_static(b"body"),
            headers,
            settled: Arc::clone(settled),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_defers_republish_to_source_with_incremented_retry_count() {
        let broker = MemoryBroker::new();
        // Subscribe before publishing: the in-memory broker does not buffer earlier messages.
        let mut sub = broker.subscribe("orders");
        let delivery = Delivery {
            retry_publisher: Some(Arc::new(broker.publisher())),
            tasks: TaskTracker::new(),
        };

        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain(&[], &settled);
        settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery)
            .await
            .unwrap();

        // The original is dropped (nack(false)), not requeued, so the broker will not redeliver it.
        assert_eq!(settled.load(Ordering::SeqCst), 1);

        // Nothing is republished before the delay elapses.
        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;

        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.payload(), b"body");
        assert_eq!(
            redelivered.headers().get_str(RETRY_COUNT_HEADER),
            Some("1"),
            "the first deferred republish must carry retry-count 1",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_increments_an_existing_retry_count() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("orders");
        let delivery = Delivery {
            retry_publisher: Some(Arc::new(broker.publisher())),
            tasks: TaskTracker::new(),
        };

        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain(&[(RETRY_COUNT_HEADER, "4")], &settled);
        settle_nack_after(msg, "orders", Duration::from_secs(1), &delivery)
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        let mut stream = std::pin::pin!(sub.stream());
        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), Some("5"));
    }

    #[tokio::test]
    async fn without_a_retry_publisher_the_fallback_requeues_immediately() {
        let delivery = Delivery::empty();
        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain(&[], &settled);
        settle_nack_after(msg, "orders", Duration::from_secs(30), &delivery)
            .await
            .unwrap();
        // No retry publisher: degrade to an immediate requeue rather than dropping silently.
        assert_eq!(settled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn native_support_defers_to_the_broker_nack_after() {
        // A native delivery: redelivered by its own timer, never through the retry publisher.
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("orders");
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("orders", b"native".as_slice()))
            .await
            .unwrap();

        // A separate broker backs the retry publisher; if the fallback fired, the republish would
        // land here and never on `sub`.
        let other = MemoryBroker::new();
        let delivery = Delivery {
            retry_publisher: Some(Arc::new(other.publisher())),
            tasks: TaskTracker::new(),
        };

        let msg = {
            let mut stream = std::pin::pin!(sub.stream());
            stream.next().await.unwrap().unwrap()
        };
        assert!(msg.supports_nack_after());
        settle_nack_after(msg, "orders", Duration::from_secs(5), &delivery)
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        let mut stream = std::pin::pin!(sub.stream());
        let redelivered = stream.next().await.unwrap().unwrap();
        // Native redelivery keeps the original payload and adds no retry-count header.
        assert_eq!(redelivered.payload(), b"native");
        assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), None);
    }
}
