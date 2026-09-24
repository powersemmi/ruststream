//! The worker pool behind `workers(n)`: a fixed set of long-lived workers per subscription, each
//! fed through a bounded channel of its own.
//!
//! The loop polls the subscription and hands what it pulls to a worker, which dispatches the
//! delivery to its end: handler, settle and continuations. The workers are tasks of the runtime
//! the loop runs on, started with their channels when the loop starts, so a delivery allocates
//! nothing on its way to one and takes no reference count: a worker borrows what the loop
//! shares for as long as it runs.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::BytesMut;
use futures::Stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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

/// How a worker says it is free for the next delivery: its index, sent back to the loop.
struct Idle {
    index: usize,
    loop_side: mpsc::Sender<usize>,
}

/// A pool worker's report that it ended mid-delivery: its index once more, so a worker that
/// panicked outside the handler's future is found by the next delivery the loop hands it,
/// rather than never picked again. A worker that leaves its loop disarms it first.
struct Farewell(Option<Idle>);

impl Drop for Farewell {
    fn drop(&mut self) {
        if let Some(idle) = &self.0 {
            // The queue has room: it holds each index at most once, and a worker's index is not
            // in it while the worker has a delivery in hand.
            let _ = idle.loop_side.try_send(idle.index);
        }
    }
}

/// The workers a loop started. Dropping it aborts them: a loop aborted by the shutdown timeout
/// takes its workers down with it, including those it was joining when the timeout hit.
struct Crew(Vec<JoinHandle<()>>);

impl Crew {
    /// Waits for every worker to end, logging the ones that failed. The handles stay in the crew
    /// until it drops, so a loop aborted mid-join still aborts the workers it had not joined.
    async fn join(&mut self) {
        for worker in &mut self.0 {
            log_worker_exit(worker.await);
        }
    }
}

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
    let mut farewell = Farewell(idle);
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
        if let Some(idle) = &farewell.0
            && idle.loop_side.send(idle.index).await.is_err()
        {
            break;
        }
    }
    // Out of the loop on its own terms: the loop is stopping or gone, and reads no more reports.
    farewell.0 = None;
}

/// A subscription's workers, started: the inbox of each and the queue free ones report on.
struct Started<Message> {
    inboxes: Vec<mpsc::Sender<Message>>,
    free: mpsc::Receiver<usize>,
    crew: Crew,
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
        crew.0.push(tokio::spawn(work(shared, inbox, idle)));
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
            // The stream first, the token only where the stream has nothing: see `turn_into`, whose
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
                        // than silently dropping what would have gone to it, or running on
                        // fewer workers than the registration asked for.
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
        crew.join().await;
        drop(free);
    })
}
