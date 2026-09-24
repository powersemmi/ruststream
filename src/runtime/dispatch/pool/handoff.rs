//! #438's feed: a bounded channel per worker; the pool hands a delivery to a free worker, the
//! lanes to the worker a key hashes to.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::BytesMut;
use futures::Stream;
use tokio::sync::mpsc;
use tracing::{debug, error};

use super::{Crew, Placement, Shared};
use crate::runtime::dispatch::{Handler, Shutdown, Turn, Workers, lane_of};
use crate::{BuildContext, IncomingMessage, Subscriber};

/// How a worker says it is free for the next delivery: its index, sent back to the loop.
struct Idle {
    index: usize,
    loop_side: mpsc::Sender<usize>,
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
        shared.handle(msg, &mut encode).await;
        // The loop keeps its end open until the workers are joined; a closed one means the loop
        // is gone, and so is anything more to do.
        if let Some(idle) = &idle
            && idle.loop_side.send(idle.index).await.is_err()
        {
            break;
        }
    }
}

/// The loop of a pool fed by handoff: at most `count` deliveries in flight, or, with `by_key`,
/// one in process and one queued per lane.
pub(super) async fn run<Sub, Body, Cx, State>(
    mut subscriber: Sub,
    shared: Arc<Shared<Body, State, Cx>>,
    shutdown: Shutdown,
    workers: Workers,
    placement: Placement,
) where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Body: Handler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let count = workers.count;
    let keyed = workers.by_key;
    // Every worker is free at the start, so the queue of free ones is seeded with all of them;
    // it never holds more.
    let (free_side, mut free) = mpsc::channel(count);
    let mut inboxes = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let (inbox_side, inbox) = mpsc::channel(1);
        let idle = (!keyed).then(|| Idle {
            index,
            loop_side: free_side.clone(),
        });
        let shared = Arc::clone(&shared);
        crew.0
            .push(placement.start(index, move || work(shared, inbox, idle)));
        inboxes.push(inbox_side);
        if !keyed {
            // Room for every worker was made above, and nothing has taken any yet.
            let _ = free_side.try_send(index);
        }
    }
    drop(free_side);
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
    crew.join().await;
    drop(free);
}
