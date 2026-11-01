//! The worker pool behind `workers(n)` on a batch subscription: the crew of the per-delivery
//! pool, fed whole batches.
//!
//! The workers start with the subscription, each with a channel of its own and the decode and
//! encode buffers its batches run with. The loop polls the subscription only with a worker free,
//! so at most `count` batches are in flight, and a batch pays one channel send: no task, no
//! reference count.

use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::BytesMut;
use futures::Stream;
use tokio::sync::mpsc;
use tracing::error;

use super::pool::{Crew, Farewell, Idle, Shared};
use super::{Shutdown, Turn, Workers};
use crate::BatchSubscriber;
use ruststream_runtime::{BatchHandler, RetryCopies, run_batch};

/// One worker: a sequential loop over the batches handed to it, with buffers of its own.
async fn work<Batch, Body, Cx, State, Retry>(
    shared: Arc<Shared<Body, State, Cx, Retry>>,
    mut inbox: mpsc::Receiver<Batch>,
    idle: Idle,
) where
    Retry: RetryCopies<Cx>,
    Body: BatchHandler<Batch, Cx, State>,
    Cx: Send + Sync + 'static,
    State: Send + Sync,
{
    let mut scratch = Body::Scratch::default();
    let mut encode = BytesMut::new();
    let mut farewell = Farewell(Some(idle));
    while let Some(batch) = inbox.recv().await {
        // Turbofish: the adapter handlers are generic over the batch context, so the worker's
        // own parameter names it.
        run_batch::<_, _, Cx, _, _, _>(
            &shared.handler,
            batch,
            &mut scratch,
            &mut encode,
            &shared.name,
            &shared.state,
            &shared.hosted.delivery,
            &shared.hosted.host,
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

/// Drives `subscriber` through `shared.handler` on `workers.count()` long-lived workers until
/// `shutdown` is triggered or the stream ends, then lets each finish its batch.
pub(super) async fn run<Sub, Body, Cx, State, Retry>(
    mut subscriber: Sub,
    shared: Arc<Shared<Body, State, Cx, Retry>>,
    shutdown: &Shutdown,
    workers: Workers,
    batch_size: NonZeroUsize,
) where
    Retry: RetryCopies<Cx>,
    Sub: BatchSubscriber + Send + 'static,
    Sub::Batch: 'static,
    Body: BatchHandler<Sub::Batch, Cx, State> + 'static,
    Cx: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let count = workers.count().get();
    // Every worker is free at the start, so the queue of free ones is seeded with all of them;
    // it never holds more.
    let (free_side, mut free) = mpsc::channel(count);
    let mut inboxes = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let (inbox_side, inbox) = mpsc::channel(1);
        let idle = Idle {
            index,
            loop_side: free_side.clone(),
        };
        crew.0
            .push(tokio::spawn(work(Arc::clone(&shared), inbox, idle)));
        inboxes.push(inbox_side);
        // Room for every worker was made above, and nothing has taken any yet.
        let _ = free_side.try_send(index);
    }
    drop(free_side);
    // The registration's own batch size, straight to the broker: whatever comes back is the
    // batch the handler sees.
    let mut stream = pin!(subscriber.batches(batch_size));
    let mut cancelled = pin!(shutdown.cancelled());
    let name = &shared.name;
    // The free worker the pool holds for the next batch: taken before the stream is polled, and
    // kept across a stream error that brought no batch.
    let mut held = None;
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let worker = if let Some(worker) = held {
            worker
        } else {
            // The token first only where no worker is free: a pool with room pays the channel
            // alone.
            let freed = poll_fn(|cx| match free.poll_recv(cx) {
                Poll::Ready(index) => Poll::Ready(index.map_or(Turn::Ended, Turn::Delivery)),
                Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match freed {
                Turn::Delivery(index) => *held.insert(index),
                // Every worker gone is a pool with nobody to hand anything to.
                Turn::Ended | Turn::Shutdown => break,
            }
        };
        // The stream first, the token only where the stream has nothing: see `turn_into`, whose
        // wait this is.
        let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match pulled {
            Turn::Delivery(Ok(batch)) => {
                held = None;
                if inboxes[worker].send(batch).await.is_err() {
                    // A worker only disappears if its task panicked; stop pulling rather than
                    // running on fewer workers than the registration asked for.
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
                shutdown.stream_ended(name);
                break;
            }
            Turn::Shutdown => break,
        }
    }
    // Closing the inboxes lets each worker finish what it holds and exit; the free queue stays
    // open until then, so none of them mistakes a late report for the end.
    drop(inboxes);
    crew.join().await;
    drop(free);
}
