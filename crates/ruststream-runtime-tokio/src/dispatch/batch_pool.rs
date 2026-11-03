//! The worker pool behind `workers(n)` on a batch subscription: the crew of the per-delivery
//! pool, fed whole batches.
//!
//! The workers start with the subscription, each with an inbox of its own and the decode and
//! encode buffers its batches run with. The loop owns one batch slot per worker: it fills the
//! slot of a free worker, hands the slot over through the worker's inbox, and the worker gives it
//! back, drained, with its report that it is free. The loop polls the subscription only with a
//! worker free, so at most `count` batches are in flight, and a batch pays one hand-off through a
//! queue that was allocated with the crew: no task, no reference count, no allocation.

use std::future::poll_fn;
use std::iter::repeat_with;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;
use ruststream_core::diag::error;

use super::pool::{Crew, Shared};
use super::{Shutdown, Turn, Workers};
use crate::BatchSubscriber;
use ruststream_core::{BatchSlot, BatchStream};
use ruststream_runtime::{BatchHandler, RetryCopies, run_batch};

/// The workers that are free for a batch, in the order they reported, each with the slot it gives
/// back: `None` for a worker that has not held one yet, or has ended.
///
/// A queue of its own rather than a channel: a channel allocates a block as its sends move on,
/// which is an allocation every few batches, and this one holds one entry per worker for good.
struct FreeList<Slot> {
    queue: ArrayQueue<(usize, Option<Slot>)>,
    reported: Notify,
}

impl<Slot> FreeList<Slot> {
    fn new(count: usize) -> Self {
        Self {
            queue: ArrayQueue::new(count),
            reported: Notify::new(),
        }
    }

    /// A worker is free. Each index is in the queue at most once (a worker reports only after
    /// it handed its batch back, and again only after the loop took it), so there is always room.
    fn report(&self, index: usize, slot: Option<Slot>) {
        let _ = self.queue.push((index, slot));
        self.reported.notify_one();
    }

    /// The next free worker. A report that lands between the pop and the wait leaves a permit
    /// behind, so the wait returns at once.
    async fn next(&self) -> (usize, Option<Slot>) {
        loop {
            if let Some(index) = self.queue.pop() {
                return index;
            }
            self.reported.notified().await;
        }
    }
}

/// The queue a worker's next slot is handed through.
///
/// A queue of one rather than a channel, for the same reason as [`FreeList`]. The loop hands a
/// slot only to a worker it has seen report free, so the queue is empty when it is filled.
struct Inbox<Slot> {
    slot: ArrayQueue<Slot>,
    arrived: Notify,
    closed: AtomicBool,
    gone: AtomicBool,
}

impl<Slot> Inbox<Slot> {
    fn new() -> Self {
        Self {
            slot: ArrayQueue::new(1),
            arrived: Notify::new(),
            closed: AtomicBool::new(false),
            gone: AtomicBool::new(false),
        }
    }

    /// Hands the worker its batch, or says it cannot: the worker ended, or its slot is taken.
    fn hand(&self, batch: Slot) -> Result<(), ()> {
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
    async fn next(&self) -> Option<Slot> {
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

/// What a worker that ends mid-batch reports through: the free list and its own inbox.
type Departure<Slot> = (Arc<FreeList<Slot>>, Arc<Inbox<Slot>>);

/// A worker's report that it ended mid-batch: its index once more, so a worker that panicked
/// outside the handler's future is found by the next batch the loop hands it, rather than never
/// picked again. A worker that leaves its loop disarms it first.
struct Farewell<Slot> {
    ended: Option<Departure<Slot>>,
    index: usize,
}

impl<Slot> Drop for Farewell<Slot> {
    fn drop(&mut self) {
        if let Some((free, inbox)) = &self.ended {
            inbox.gone.store(true, Ordering::Release);
            free.report(self.index, None);
        }
    }
}

/// One worker: a sequential loop over the batches handed to it, with buffers of its own.
async fn work<Slot, Body, Cx, State, Retry>(
    shared: Arc<Shared<Body, State, Cx, Retry>>,
    inbox: Arc<Inbox<Slot>>,
    free: Arc<FreeList<Slot>>,
    index: usize,
    batch_size: NonZeroUsize,
) where
    Retry: RetryCopies<Cx>,
    Body: BatchHandler<Slot, Cx, State>,
    Cx: Send + Sync + 'static,
    State: Send + Sync,
{
    let mut farewell = Farewell {
        ended: Some((Arc::clone(&free), Arc::clone(&inbox))),
        index,
    };
    let mut taken = Body::taken(batch_size);
    let mut scratch = Body::Scratch::default();
    let mut encode = BytesMut::new();
    while let Some(mut slot) = inbox.next().await {
        // Turbofish: the adapter handlers are generic over the batch context, so the worker's
        // own parameter names it.
        run_batch::<_, _, Cx, _, _, _>(
            &shared.handler,
            &mut slot,
            &mut taken,
            &mut scratch,
            &mut encode,
            &shared.name,
            &shared.state,
            &shared.hosted.delivery,
            &shared.hosted.host,
            &shared.failure,
        )
        .await;
        // The slot goes back drained, with the report that this worker is free for the next one.
        free.report(index, Some(slot));
    }
    // Out of the loop on its own terms: the loop is stopping or gone, and reads no more reports.
    farewell.ended = None;
}

/// Waits for a free worker and takes its slot, or `None` once shutdown fires or the worker is gone.
///
/// The token is polled first only where no worker is free: a pool with room pays the queue alone.
/// A worker that ends reports itself once more, so the wait always has one to find.
async fn acquire<Slot, Cancelled>(
    free: &FreeList<Slot>,
    slots: &mut [Option<Slot>],
    cancelled: &mut Pin<&mut Cancelled>,
    name: &str,
) -> Option<(usize, Slot)>
where
    Cancelled: Future<Output = ()>,
{
    let freed = {
        let mut next = pin!(free.next());
        poll_fn(|cx| match next.as_mut().poll(cx) {
            Poll::Ready(freed) => Poll::Ready(Turn::Delivery(freed)),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await
    };
    let Turn::Delivery((index, returned)) = freed else {
        return None;
    };
    if returned.is_some() {
        slots[index] = returned;
    }
    let slot = slots[index].take();
    if slot.is_none() {
        // A worker only disappears if its task panicked, taking its slot with it; stop pulling
        // rather than running on fewer workers than the registration asked for.
        error!(
            target: "ruststream::dispatch",
            subscription = %name,
            worker = index,
            "worker terminated; stopping dispatch",
        );
    }
    slot.map(|slot| (index, slot))
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
    // it never holds more. Each worker's slot is the loop's until it hands it over.
    let free = Arc::new(FreeList::new(count));
    let mut slots: Vec<Option<Sub::Batch>> =
        repeat_with(|| Some(<Sub::Batch as BatchSlot>::with_capacity(batch_size)))
            .take(count)
            .collect();
    let mut inboxes = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let inbox = Arc::new(Inbox::new());
        crew.0.push(tokio::spawn(work(
            Arc::clone(&shared),
            Arc::clone(&inbox),
            Arc::clone(&free),
            index,
            batch_size,
        )));
        inboxes.push(inbox);
        free.report(index, None);
    }
    // The registration's own batch size, straight to the broker: whatever comes back is the
    // batch the handler sees.
    let mut stream = pin!(subscriber.batches(batch_size));
    let mut cancelled = pin!(shutdown.cancelled());
    let name = &shared.name;
    // The free worker and its slot the pool holds for the next batch: taken before the stream is
    // polled, and kept across a stream error that brought no batch.
    let mut held: Option<(usize, Sub::Batch)> = None;
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let (worker, mut slot) = if let Some(held) = held.take() {
            held
        } else {
            let Some(acquired) = acquire(&free, &mut slots, &mut cancelled, name).await else {
                break;
            };
            acquired
        };
        // The stream first, the token only where the stream has nothing: see `turn_into`, whose
        // wait this is.
        let pulled = poll_fn(|cx| match stream.as_mut().poll_fill(cx, &mut slot) {
            Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match pulled {
            Turn::Delivery(Ok(())) => {
                if inboxes[worker].hand(slot).is_err() {
                    // The worker ended between reporting free and being handed the batch.
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
                // A failed fill leaves the slot empty; draining it keeps that true of a broker
                // that left a message behind.
                slot.drain().for_each(drop);
                held = Some((worker, slot));
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
