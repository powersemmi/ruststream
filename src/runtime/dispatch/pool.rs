//! The worker pool behind `workers(n)`: a fixed set of long-lived workers per subscription, each
//! fed through a bounded channel of its own and placed, once, on the thread it runs on.
//!
//! The loop that polls the subscription stays where it was spawned. What it pulls goes to a
//! worker, which dispatches the delivery to its end: handler, settle and continuations. The
//! workers and their channels are made at startup, so a delivery allocates nothing on its way
//! to one and takes no reference count: a worker borrows what the loop shares for as long as it
//! runs.

use std::future::{Future, poll_fn};
use std::mem;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::thread::available_parallelism;

use bytes::BytesMut;
use futures::Stream;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::task::LocalPoolHandle;
use tracing::{debug, error};

use super::{
    Delivery, DispatchFailure, Handler, Shutdown, Slot, Turn, Workers, dispatch, lane_of,
    log_worker_exit,
};
use crate::{BuildContext, IncomingMessage, Subscriber};

/// What the loop shares with every worker it starts, behind one reference count taken once per
/// worker.
struct Shared<Body, State, Cx> {
    handler: Arc<Body>,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery<Cx>>,
    failure: DispatchFailure,
}

/// Where a subscription's workers run, decided once, when the loop starts.
enum Placement {
    /// As tasks of the runtime the loop runs on.
    ///
    /// Under the test harness, whose clock and quiescence wait have to reach every timer and
    /// every delivery of the app, and on a current-thread runtime, which has one thread to offer.
    Runtime,
    /// Each worker pinned to a thread of a pool of the subscription's own, so the worker's state
    /// and the deliveries it dispatches stay on one core.
    Pinned(LocalPoolHandle),
}

impl Placement {
    /// The placement for a pool of `count` workers.
    fn of<Cx>(delivery: &Delivery<Cx>, count: usize) -> Self {
        // A harness run keeps the app on the test's runtime: `advance` moves that runtime's
        // clock only, and a worker on a thread of its own would arm its timers on another.
        #[cfg(feature = "testing")]
        if delivery.hooks.coordinator().is_some() {
            return Self::Runtime;
        }
        #[cfg(not(feature = "testing"))]
        let _ = delivery;
        if Handle::current().runtime_flavor() == RuntimeFlavor::CurrentThread {
            return Self::Runtime;
        }
        // More threads than cores would only take turns on them; the workers share the pool's
        // threads round-robin instead.
        let threads = available_parallelism().map_or(1, usize::from).min(count);
        Self::Pinned(LocalPoolHandle::new(threads))
    }

    /// Starts worker `index`.
    fn start<Work, Fut>(&self, index: usize, work: Work) -> JoinHandle<()>
    where
        Work: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match self {
            Self::Runtime => tokio::spawn(work()),
            Self::Pinned(pool) => pool.spawn_pinned_by_idx(work, index % pool.num_threads()),
        }
    }
}

/// How a worker says it is free for the next delivery: its index, sent back to the loop.
struct Idle {
    index: usize,
    loop_side: mpsc::Sender<usize>,
}

/// The workers a loop started. Dropping it aborts them: a loop aborted by the shutdown timeout
/// takes its workers down with it.
struct Crew(Vec<JoinHandle<()>>);

impl Drop for Crew {
    fn drop(&mut self) {
        for worker in &self.0 {
            worker.abort();
        }
    }
}

/// One worker: a sequential loop over the deliveries handed to it.
async fn work<Message, Body, Cx, State>(
    shared: Arc<Shared<Body, State, Cx>>,
    mut inbox: mpsc::Receiver<Message>,
    idle: Option<Idle>,
) where
    Message: IncomingMessage,
    Body: Handler<Message, Cx, State>,
    Cx: BuildContext<Message> + Send + Sync + 'static,
    State: Send + Sync,
{
    // One encode buffer per worker, for the reason the sequential loop has one: a worker is a
    // sequential loop over what it is handed.
    let mut encode = BytesMut::new();
    while let Some(msg) = inbox.recv().await {
        let mut slot = Slot::new(msg);
        dispatch(
            &*shared.handler,
            &mut slot,
            &mut encode,
            &shared.name,
            &shared.state,
            &shared.delivery,
            &shared.failure,
        )
        .await;
        // The loop keeps its end open until the workers are joined; a closed one means the loop
        // is gone, and so is anything more to do.
        if let Some(idle) = &idle
            && idle.loop_side.send(idle.index).await.is_err()
        {
            break;
        }
    }
}

/// A subscription's workers, started: the inbox of each, the queue free ones report on, and the
/// placement whose threads they run on (a pinned pool lives as long as this does).
struct Started<Message> {
    inboxes: Vec<mpsc::Sender<Message>>,
    free: mpsc::Receiver<usize>,
    crew: Crew,
    placement: Placement,
}

/// Starts `workers.count` workers over `shared`, reporting when free unless they are keyed lanes.
fn start<Message, Body, Cx, State>(
    shared: &Arc<Shared<Body, State, Cx>>,
    workers: Workers,
) -> Started<Message>
where
    Message: IncomingMessage + Send + 'static,
    Body: Handler<Message, Cx, State> + 'static,
    Cx: BuildContext<Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let count = workers.count;
    let placement = Placement::of(&shared.delivery, count);
    // Every worker is free at the start, so the queue of free ones is seeded with all of them;
    // it never holds more.
    let (free_side, free) = mpsc::channel(count);
    let mut inboxes = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let (inbox_side, inbox) = mpsc::channel(1);
        let idle = (!workers.by_key).then(|| Idle {
            index,
            loop_side: free_side.clone(),
        });
        let shared = Arc::clone(shared);
        crew.0
            .push(placement.start(index, move || work(shared, inbox, idle)));
        inboxes.push(inbox_side);
        if !workers.by_key {
            // Room for every worker was made above, and nothing has taken any yet.
            let _ = free_side.try_send(index);
        }
    }
    Started {
        inboxes,
        free,
        crew,
        placement,
    }
}

/// Spawns the loop of a subscription with `workers.count` workers: a pool that hands each
/// delivery to a free worker, or, with `by_key`, lanes that hand it to the worker its key hashes
/// to.
///
/// The pool polls the stream only with a worker free for what it yields, so at most `count`
/// deliveries are in flight. A lane holds one delivery in process and one queued, and the loop
/// waits on a busy lane rather than reorder its key.
// See `spawn_dispatch_workers`: each part is the registration's own.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_dispatch_pool<Sub, Body, Cx, State>(
    mut subscriber: Sub,
    handler: Arc<Body>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery<Cx>>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Body: Handler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let count = workers.count;
        let keyed = workers.by_key;
        let shared = Arc::new(Shared {
            handler,
            name,
            state,
            delivery,
            failure,
        });
        let Started {
            inboxes,
            mut free,
            mut crew,
            placement,
        } = start(&shared, workers);
        let name = &shared.name;
        let mut stream = pin!(subscriber.stream());
        let mut cancelled = pin!(shutdown.cancelled());
        let mut rotation = 0usize;
        // The free worker the pool holds for the next delivery: taken before the stream is
        // polled, and kept across a stream error that brought no delivery.
        let mut held = None;
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            if !keyed && held.is_none() {
                // The token first only where no worker is free: a pool with room pays the
                // channel alone.
                let freed = poll_fn(|cx| match free.poll_recv(cx) {
                    Poll::Ready(index) => Poll::Ready(index.map_or(Turn::Ended, Turn::Delivery)),
                    Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
                })
                .await;
                match freed {
                    Turn::Delivery(index) => held = Some(index),
                    // Every worker gone is a pool with nobody to hand anything to.
                    Turn::Ended | Turn::Shutdown => break,
                }
            }
            // The stream first, the token only where the stream has nothing: see `turn`, whose
            // wait this is.
            let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
                Poll::Ready(None) => Poll::Ready(Turn::Ended),
                Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match pulled {
                Turn::Delivery(Ok(msg)) => {
                    let worker = held.take().unwrap_or_else(|| {
                        // No key: any lane will do; rotate to spread the load.
                        msg.partition_key().map_or_else(
                            || {
                                rotation = (rotation + 1) % count;
                                rotation
                            },
                            |key| lane_of(key, count),
                        )
                    });
                    if inboxes[worker].send(msg).await.is_err() {
                        // A worker only disappears if its task panicked; stop pulling rather
                        // than silently dropping what would have gone to it.
                        error!(
                            target: "ruststream::dispatch",
                            subscription = %name,
                            worker,
                            "worker terminated; stopping dispatch",
                        );
                        break;
                    }
                }
                Turn::Delivery(Err(err)) => {
                    error!(
                        target: "ruststream::dispatch",
                        subscription = %name,
                        error = %err,
                        "subscriber stream error",
                    );
                }
                Turn::Ended => {
                    debug!(
                        target: "ruststream::dispatch",
                        subscription = %name,
                        "subscriber stream ended",
                    );
                    break;
                }
                Turn::Shutdown => break,
            }
        }
        // Closing the inboxes lets each worker finish what it holds and exit; the free queue
        // stays open until then, so none of them mistakes a late report for the end.
        drop(inboxes);
        for worker in mem::take(&mut crew.0) {
            log_worker_exit(worker.await);
        }
        // The pinned pool's threads end here, after every worker on them has.
        drop(free);
        drop(placement);
    })
}
