//! The worker pool behind `workers(n)` on a batch subscription: the crew of the per-delivery
//! pool, fed whole batches.
//!
//! The workers start with the subscription, each with an inbox of its own and the decode and
//! encode buffers its batches run with. The loop polls the subscription only with a worker free,
//! so at most `count` batches are in flight, and a batch pays one hand-off through a slot that
//! was allocated with the crew: no task, no reference count, no allocation.

use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use futures::Stream;
use tokio::sync::Notify;
use tracing::error;

use super::pool::{Crew, Shared};
use super::{Shutdown, Turn, Workers};
use crate::BatchSubscriber;
use ruststream_runtime::{BatchHandler, RetryCopies, run_batch};

/// The workers that are free for a batch, in the order they reported.
///
/// A queue of its own rather than a channel: a channel allocates a block as its sends move on,
/// which is an allocation every few batches, and this one holds one slot per worker for good.
struct FreeList {
    queue: ArrayQueue<usize>,
    reported: Notify,
}

impl FreeList {
    fn new(count: usize) -> Self {
        Self {
            queue: ArrayQueue::new(count),
            reported: Notify::new(),
        }
    }

    /// A worker is free. Each index is in the queue at most once (a worker reports only after
    /// it handed its batch back, and again only after the loop took it), so there is always room.
    fn report(&self, index: usize) {
        let _ = self.queue.push(index);
        self.reported.notify_one();
    }

    /// The next free worker. A report that lands between the pop and the wait leaves a permit
    /// behind, so the wait returns at once.
    async fn next(&self) -> usize {
        loop {
            if let Some(index) = self.queue.pop() {
                return index;
            }
            self.reported.notified().await;
        }
    }
}

/// The slot a worker's next batch is handed through.
///
/// A queue of one rather than a channel, for the same reason as [`FreeList`]. The loop hands a
/// batch only to a worker it has seen report free, so the slot is empty when it is filled.
struct Inbox<Batch> {
    slot: ArrayQueue<Batch>,
    arrived: Notify,
    closed: AtomicBool,
    gone: AtomicBool,
}

impl<Batch> Inbox<Batch> {
    fn new() -> Self {
        Self {
            slot: ArrayQueue::new(1),
            arrived: Notify::new(),
            closed: AtomicBool::new(false),
            gone: AtomicBool::new(false),
        }
    }

    /// Hands the worker its batch, or says it cannot: the worker ended, or its slot is taken.
    fn hand(&self, batch: Batch) -> Result<(), ()> {
        // The worker marks itself gone before it reports free, and the loop learns of the free
        // worker through the queue, so a worker that ended is seen ended here.
        if self.gone.load(Ordering::Acquire) || self.slot.push(batch).is_err() {
            return Err(());
        }
        self.arrived.notify_one();
        Ok(())
    }

    /// No more batches: the worker finishes the one it holds, drains the slot and ends.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.arrived.notify_one();
    }

    /// The next batch, or `None` once the inbox is closed and empty. The flag is read before the
    /// slot, so a batch handed before the close is never missed.
    async fn next(&self) -> Option<Batch> {
        loop {
            let closed = self.closed.load(Ordering::Acquire);
            if let Some(batch) = self.slot.pop() {
                return Some(batch);
            }
            if closed {
                return None;
            }
            self.arrived.notified().await;
        }
    }
}

/// A worker's report that it ended mid-batch: its index once more, so a worker that panicked
/// outside the handler's future is found by the next batch the loop hands it, rather than never
/// picked again. A worker that leaves its loop disarms it first.
struct Farewell<Batch> {
    ended: Option<(Arc<FreeList>, Arc<Inbox<Batch>>)>,
    index: usize,
}

impl<Batch> Drop for Farewell<Batch> {
    fn drop(&mut self) {
        if let Some((free, inbox)) = &self.ended {
            inbox.gone.store(true, Ordering::Release);
            free.report(self.index);
        }
    }
}

/// One worker: a sequential loop over the batches handed to it, with buffers of its own.
async fn work<Batch, Body, Cx, State, Retry>(
    shared: Arc<Shared<Body, State, Cx, Retry>>,
    inbox: Arc<Inbox<Batch>>,
    free: Arc<FreeList>,
    index: usize,
) where
    Retry: RetryCopies<Cx>,
    Body: BatchHandler<Batch, Cx, State>,
    Cx: Send + Sync + 'static,
    State: Send + Sync,
{
    let mut farewell = Farewell {
        ended: Some((Arc::clone(&free), Arc::clone(&inbox))),
        index,
    };
    let mut scratch = Body::Scratch::default();
    let mut encode = BytesMut::new();
    while let Some(batch) = inbox.next().await {
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
        free.report(index);
    }
    // Out of the loop on its own terms: the loop is stopping or gone, and reads no more reports.
    farewell.ended = None;
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
    let free = Arc::new(FreeList::new(count));
    let mut inboxes = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let inbox = Arc::new(Inbox::new());
        crew.0.push(tokio::spawn(work(
            Arc::clone(&shared),
            Arc::clone(&inbox),
            Arc::clone(&free),
            index,
        )));
        inboxes.push(inbox);
        free.report(index);
    }
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
            // The token first only where no worker is free: a pool with room pays the queue
            // alone. A worker that ends reports itself once more, so the wait always has one
            // to find.
            let freed = {
                let mut next = pin!(free.next());
                poll_fn(|cx| match next.as_mut().poll(cx) {
                    Poll::Ready(index) => Poll::Ready(Turn::Delivery(index)),
                    Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
                })
                .await
            };
            match freed {
                Turn::Delivery(index) => *held.insert(index),
                _ => break,
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
                if inboxes[worker].hand(batch).is_err() {
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
    for inbox in &inboxes {
        inbox.close();
    }
    crew.join().await;
}
