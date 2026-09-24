//! One bounded MPMC queue in front of the workers: the loop keeps it topped up, a worker drains it
//! and parks only when it finds it empty, and the loop wakes a worker only when it knows one is
//! parked.
//!
//! Per delivery, under load: one CAS to push and one to pop (`crossbeam_queue::ArrayQueue`, a
//! preallocated ring), one load of the parked set by the loop, one load of the loop's waiting flag
//! by the worker; no allocation, no wake. A wake is paid on the idle-to-busy edge of a worker and,
//! for the loop, once per drain of the queue to its low-water mark. Read-ahead: `n` deliveries in
//! process plus the queue's capacity.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use futures::Stream;
use futures::task::AtomicWaker;
use tracing::{debug, error};

use super::research::{Knobs, Spin};
use super::{Crew, Placement, Shared};
use crate::runtime::dispatch::{Handler, Shutdown, Turn, Workers};
use crate::{BuildContext, IncomingMessage, Subscriber};

/// What the loop and its workers share: the queue and who waits on it.
struct Feed<Message> {
    queue: ArrayQueue<Message>,
    /// The loop is woken for room once the queue has drained to this length.
    low_water: usize,
    /// The workers parked on an empty queue, one bit each.
    parked: Box<[CachePadded<AtomicU64>]>,
    wakers: Box<[CachePadded<AtomicWaker>]>,
    /// Set once the loop has stopped pulling: the workers drain the queue and exit.
    closed: CachePadded<AtomicBool>,
    /// Set by the loop while it waits for room.
    loop_waiting: CachePadded<AtomicBool>,
    loop_waker: AtomicWaker,
    /// The workers still running; the last one out wakes the loop.
    alive: AtomicUsize,
}

impl<Message> Feed<Message> {
    fn new(count: usize, capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            low_water: capacity / 2,
            parked: (0..count.div_ceil(64))
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            wakers: (0..count)
                .map(|_| CachePadded::new(AtomicWaker::new()))
                .collect(),
            closed: CachePadded::new(AtomicBool::new(false)),
            loop_waiting: CachePadded::new(AtomicBool::new(false)),
            loop_waker: AtomicWaker::new(),
            alive: AtomicUsize::new(count),
        }
    }

    /// The next delivery, waking the loop if it waits for room and the queue fell to low water.
    fn take(&self) -> Option<Message> {
        let msg = self.queue.pop()?;
        // The pop's CAS is sequentially consistent, and so is this load: the loop that set the
        // flag after this pop re-checks the queue and sees it.
        if self.loop_waiting.load(Ordering::SeqCst)
            && self.queue.len() <= self.low_water
            && self.loop_waiting.swap(false, Ordering::SeqCst)
        {
            self.loop_waker.wake();
        }
        Some(msg)
    }

    /// Parks worker `index` until the queue has something or the loop is gone. `Ready(false)`
    /// once the queue is closed and empty.
    fn park(&self, index: usize, cx: &TaskContext<'_>) -> Poll<bool> {
        self.wakers[index].register(cx.waker());
        let (word, bit) = (index / 64, 1u64 << (index % 64));
        self.parked[word].fetch_or(bit, Ordering::SeqCst);
        // Checked after the bit is set: a push the loop made before reading the set is seen
        // here, and one it makes after finds the bit.
        if !self.queue.is_empty() || self.closed.load(Ordering::SeqCst) {
            self.parked[word].fetch_and(!bit, Ordering::SeqCst);
            return Poll::Ready(!self.queue.is_empty());
        }
        Poll::Pending
    }

    /// Wakes one parked worker, if the loop knows of one.
    fn wake_one(&self) {
        for (word_index, word) in self.parked.iter().enumerate() {
            let mut bits = word.load(Ordering::SeqCst);
            while bits != 0 {
                let bit = bits & bits.wrapping_neg();
                let before = word.fetch_and(!bit, Ordering::SeqCst);
                if before & bit != 0 {
                    self.wakers[word_index * 64 + bit.trailing_zeros() as usize].wake();
                    return;
                }
                bits = before & !bit;
            }
        }
    }

    /// Room for one more delivery; `Ready(false)` once every worker is gone.
    fn poll_room(&self, cx: &TaskContext<'_>) -> Poll<bool> {
        if self.alive.load(Ordering::SeqCst) == 0 {
            return Poll::Ready(false);
        }
        if !self.queue.is_full() {
            return Poll::Ready(true);
        }
        self.loop_waker.register(cx.waker());
        self.loop_waiting.store(true, Ordering::SeqCst);
        if !self.queue.is_full() {
            self.loop_waiting.store(false, Ordering::SeqCst);
            return Poll::Ready(true);
        }
        if self.alive.load(Ordering::SeqCst) == 0 {
            return Poll::Ready(false);
        }
        Poll::Pending
    }

    /// Stops the workers once they have drained the queue.
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        for waker in &*self.wakers {
            waker.wake();
        }
    }
}

/// Counts a worker out when it ends, however it ends, and wakes the loop when the last one goes.
struct Alive<'a, Message>(&'a Feed<Message>);

impl<Message> Drop for Alive<'_, Message> {
    fn drop(&mut self) {
        if self.0.alive.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.loop_waker.wake();
        }
    }
}

/// One worker: drains the queue, parks when it is empty, exits when it is closed and empty.
async fn work<Message, Body, Cx, State>(
    shared: Arc<Shared<Body, State, Cx>>,
    feed: Arc<Feed<Message>>,
    index: usize,
    spin: Spin,
) where
    Message: IncomingMessage,
    Body: Handler<Message, Cx, State>,
    Cx: BuildContext<Message> + Send + Sync + 'static,
    State: Send + Sync,
{
    let _alive = Alive(&feed);
    let mut encode = BytesMut::new();
    loop {
        if let Some(msg) = feed.take() {
            shared.handle(msg, &mut encode).await;
            continue;
        }
        if spin.wait(|| !feed.queue.is_empty()).await {
            continue;
        }
        if !poll_fn(|cx| feed.park(index, cx)).await {
            break;
        }
    }
}

/// The loop of a pool fed through one shared queue.
pub(super) async fn run<Sub, Body, Cx, State>(
    mut subscriber: Sub,
    shared: Arc<Shared<Body, State, Cx>>,
    shutdown: Shutdown,
    workers: Workers,
    placement: Placement,
    knobs: Knobs,
) where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Body: Handler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let count = workers.count;
    let feed = Arc::new(Feed::new(count, knobs.capacity(count)));
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let shared = Arc::clone(&shared);
        let feed = Arc::clone(&feed);
        let spin = knobs.spin;
        crew.0
            .push(placement.start(index, move || work(shared, feed, index, spin)));
    }
    let name = &shared.name;
    let mut stream = pin!(subscriber.stream());
    let mut cancelled = pin!(shutdown.cancelled());
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        // Room first, so nothing is pulled off the stream that the queue could not take.
        let room = poll_fn(|cx| match feed.poll_room(cx) {
            Poll::Ready(true) => Poll::Ready(Turn::Delivery(())),
            Poll::Ready(false) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match room {
            Turn::Delivery(()) => {}
            Turn::Ended => {
                error!(
                    target: "ruststream::dispatch",
                    subscription = %name,
                    "every worker terminated; stopping dispatch",
                );
                break;
            }
            Turn::Shutdown => break,
        }
        let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match pulled {
            Turn::Delivery(Ok(msg)) => {
                // Only the loop pushes, and it waited for room: room only grows until it does.
                if feed.queue.push(msg).is_err() {
                    unreachable!("the loop pushes only after it saw room");
                }
                feed.wake_one();
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
    // What the queue holds is handled and settled before the workers exit: nothing pulled off
    // the stream is dropped unsettled unless the shutdown timeout aborts the loop.
    feed.close();
    crew.join().await;
}
