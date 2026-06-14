//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. Lifted out of the former `Router` so
//! [`RustStream`](super::RustStream) can own task spawning directly.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};

use crate::{BatchSubscriber, Headers, IncomingMessage, Subscriber};

use super::batch::BatchHandler;
use super::context::{Context, State};
use super::handler::Handler;
use super::publish::PublishMiddleware;
use super::publisher_registry::ErasedPublisher;

/// Named publishers registered on the application, resolvable from a [`Context`] by name.
pub(crate) type Publishers = HashMap<String, Arc<dyn ErasedPublisher>>;

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

/// Per-scope publish context threaded into every delivery's [`Context`]: the named-publisher
/// registry, the scope's publish middleware pipeline, and the app-wide tracker for post-settle
/// continuations. A handler resolves a publisher with [`Context::publisher`] and its sends run
/// through `pipeline`, the same chain as a macro reply; an `and_after` continuation is spawned onto
/// `tasks` so a graceful shutdown drains it.
pub(crate) struct Delivery {
    pub(crate) publishers: Publishers,
    pub(crate) pipeline: Arc<[Arc<dyn PublishMiddleware>]>,
    pub(crate) tasks: TaskTracker,
}

impl Delivery {
    /// An empty delivery context: no publishers, no pipeline, a fresh continuation tracker. For
    /// tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::with_tasks(TaskTracker::new())
    }

    /// An empty delivery context carrying a caller-owned continuation tracker, so a test can
    /// observe the post-settle continuations spawned through it.
    #[cfg(test)]
    pub(crate) fn with_tasks(tasks: TaskTracker) -> Self {
        Self {
            publishers: HashMap::new(),
            pipeline: Arc::from([]),
            tasks,
        }
    }
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("publishers", &self.publishers.len())
            .field("layers", &self.pipeline.len())
            .field("pending_continuations", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates. Each delivery is given a [`Context`] built from `name`, the message headers,
/// shared `state`, and the `delivery` publish context.
pub(crate) fn spawn_dispatch<S, H>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery>,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    H: Handler<S::Message> + 'static,
{
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(subscriber.stream());
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                next = stream.next() => match next {
                    Some(Ok(msg)) => dispatch(&*handler, msg, &name, &state, &delivery).await,
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
    })
}

/// Spawns a task that drives `subscriber` through `handler` with a bounded worker pool: up to
/// `workers.count` deliveries in flight, each handled (and settled) in its own task. With
/// `by_key`, the pool becomes per-key sequential lanes instead.
///
/// Sequential policies delegate to [`spawn_dispatch`]. On shutdown the stream stops being
/// polled and in-flight workers drain; if the app's `shutdown_timeout` aborts this task, the
/// owned worker tasks abort with it.
pub(crate) fn spawn_dispatch_workers<S, H>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery>,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message> + 'static,
{
    if workers.is_sequential() {
        return spawn_dispatch(subscriber, handler, shutdown, name, state, delivery);
    }
    if workers.by_key {
        spawn_dispatch_lanes(
            subscriber, handler, shutdown, name, state, delivery, workers,
        )
    } else {
        spawn_dispatch_pool(
            subscriber, handler, shutdown, name, state, delivery, workers,
        )
    }
}

fn spawn_dispatch_pool<S, H>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery>,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message> + 'static,
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
                        tasks.spawn(async move {
                            dispatch(&*handler, msg, &name, &state, &delivery).await;
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

fn spawn_dispatch_lanes<S, H>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery>,
    workers: Workers,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message> + 'static,
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
            tasks.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    dispatch(&*handler, msg, &name, &state, &delivery).await;
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
pub(crate) fn spawn_batch_dispatch<S, H>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery>,
    workers: Workers,
) -> JoinHandle<()>
where
    S: BatchSubscriber + Send + 'static,
    S::Message: Send + 'static,
    H: BatchHandler<S::Message> + 'static,
{
    tokio::spawn(async move {
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
                            // A batch has no single working copy of headers, so the context
                            // starts with an empty set; per-message headers stay on the broker
                            // deliveries.
                            let empty = Headers::new();
                            let mut ctx = Context::new(&name, &empty, &state, &delivery);
                            handler.handle_batch(batch, &mut ctx).await;
                        } else {
                            let handler = Arc::clone(&handler);
                            let name = Arc::clone(&name);
                            let state = Arc::clone(&state);
                            let delivery = Arc::clone(&delivery);
                            tasks.spawn(async move {
                                let empty = Headers::new();
                                let mut ctx = Context::new(&name, &empty, &state, &delivery);
                                handler.handle_batch(batch, &mut ctx).await;
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

async fn dispatch<H, M>(handler: &H, msg: M, name: &str, state: &State, delivery: &Delivery)
where
    H: Handler<M>,
    M: IncomingMessage,
{
    let mut ctx = Context::new(name, msg.headers(), state, delivery);
    let mut settle = handler.handle(&msg, &mut ctx).await;
    let after = settle.take_after();
    super::batch::settle(msg, settle.outcome(), name).await;
    // Run the post-settle continuation on the tracked set so a graceful shutdown drains it.
    // At-most-once: the message is already settled, so a lost or panicking continuation never
    // redelivers it.
    if let Some(after) = after {
        delivery.tasks.spawn(after);
    }
}

#[cfg(all(test, feature = "memory", feature = "json"))]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use tokio::sync::Notify;

    use super::super::handler::{HandlerResult, Settle};
    use super::*;
    use crate::memory::MemoryBroker;
    use crate::{OutgoingMessage, Publisher, Subscriber};

    async fn publish_one(broker: &MemoryBroker, name: &str) {
        broker
            .publisher()
            .publish(OutgoingMessage::new(
                name,
                &serde_json::to_vec(&1u32).unwrap(),
            ))
            .await
            .unwrap();
    }

    // After ANY settle the continuation runs: an ack with `and_after`, and a drop with `and_after`,
    // both fire on the tracked set once the message is settled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_continuation_runs_after_ack_and_after_drop() {
        for outcome in [HandlerResult::ack(), HandlerResult::drop()] {
            let broker = MemoryBroker::new();
            let mut sub = broker.subscribe("after-single");
            publish_one(&broker, "after-single").await;

            let ran = Arc::new(Notify::new());
            let signal = Arc::clone(&ran);
            let handler = move |_msg: &crate::memory::MemoryMessage, _ctx: &mut Context| {
                let signal = Arc::clone(&signal);
                async move {
                    let settle: Settle = outcome.and_after(async move { signal.notify_one() });
                    settle
                }
            };

            let tasks = TaskTracker::new();
            let state = State::default();
            let delivery = Delivery::with_tasks(tasks.clone());
            let mut stream = std::pin::pin!(sub.stream());
            let msg = stream.next().await.unwrap().unwrap();
            dispatch(&handler, msg, "after-single", &state, &delivery).await;

            // The continuation ran after settling, regardless of ack or drop.
            ran.notified().await;
            tasks.close();
            tasks.wait().await;

            // The message was settled (acked or dropped), so nothing is redelivered.
            assert!(futures::poll!(stream.next()).is_pending());
        }
    }
}
