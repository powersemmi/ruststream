//! A bounded SPSC ring per worker: the loop pushes each delivery to one ring (round-robin, the ring
//! with the most room, or the ring a key hashes to) and wakes that worker only if it is parked; a
//! worker drains its ring and parks only when it finds it empty.
//!
//! Per delivery, under load: a plain store to push and one to pop (`rtrb`, a preallocated ring
//! whose sides read each other's index only when their cached copy runs out, so a worker reads the
//! loop's index once per burst it drains), and one `SeqCst` fence on each side for the park and
//! room handshakes; no allocation, no wake. Picking the ring with the most room reads every
//! worker's index, `n` cross-core loads per delivery; round-robin reads none. Read-ahead: per
//! ring, one delivery in process plus the ring's capacity. A key always goes to the same ring, and
//! a ring is drained in order by one worker, so per-key order holds; the loop waits on a full ring
//! rather than reorder a key.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::task::{Context as TaskContext, Poll};

use bytes::BytesMut;
use crossbeam_utils::CachePadded;
use futures::Stream;
use futures::task::AtomicWaker;
use rtrb::{Consumer, Producer, RingBuffer};
use tracing::{debug, error};

use super::research::{Knobs, Spin};
use super::{Crew, Placement, Shared};
use crate::runtime::dispatch::{Handler, Shutdown, Turn, Workers, lane_of};
use crate::{BuildContext, IncomingMessage, Subscriber};

/// Who waits on whom: a worker on its empty ring, the loop on room.
struct Signals {
    parked: Box<[CachePadded<AtomicBool>]>,
    wakers: Box<[CachePadded<AtomicWaker>]>,
    loop_waiting: CachePadded<AtomicBool>,
    loop_waker: AtomicWaker,
    /// The workers still running; the last one out wakes the loop.
    alive: AtomicUsize,
}

/// Which ring the loop waits on for room.
#[derive(Clone, Copy)]
enum Which {
    /// This one: the next in turn, or the one a key hashes to.
    One(usize),
    /// Whichever has the most room, ties going to the first from here.
    Most(usize),
}

impl Signals {
    fn new(count: usize) -> Self {
        Self {
            parked: (0..count)
                .map(|_| CachePadded::new(AtomicBool::new(false)))
                .collect(),
            wakers: (0..count)
                .map(|_| CachePadded::new(AtomicWaker::new()))
                .collect(),
            loop_waiting: CachePadded::new(AtomicBool::new(false)),
            loop_waker: AtomicWaker::new(),
            alive: AtomicUsize::new(count),
        }
    }

    /// A worker freed a slot of its ring: wakes the loop if it waits for room.
    fn freed(&self) {
        // Orders the ring's index store before the flag's load, against the loop's flag store
        // before its ring load.
        fence(Ordering::SeqCst);
        if self.loop_waiting.load(Ordering::Relaxed)
            && self.loop_waiting.swap(false, Ordering::SeqCst)
        {
            self.loop_waker.wake();
        }
    }

    /// The loop pushed to ring `index`: wakes its worker if it is parked.
    fn pushed(&self, index: usize) {
        // Orders the ring's index store before the parked flag's load, against the worker's flag
        // store before its ring load.
        fence(Ordering::SeqCst);
        if self.parked[index].load(Ordering::Relaxed)
            && self.parked[index].swap(false, Ordering::SeqCst)
        {
            self.wakers[index].wake();
        }
    }

    /// The ring with room the loop waits for, by `which`; `Ready(None)` once every worker is gone.
    fn poll_room<Message>(
        &self,
        rings: &[Producer<Message>],
        which: Which,
        cx: &TaskContext<'_>,
    ) -> Poll<Option<usize>> {
        let pick = || match which {
            Which::One(index) => (!rings[index].is_full()).then_some(index),
            Which::Most(start) => {
                let count = rings.len();
                let mut best: Option<(usize, usize)> = None;
                for offset in 0..count {
                    let index = (start + offset) % count;
                    let room = rings[index].slots();
                    if room > best.map_or(0, |(_, most)| most) {
                        best = Some((index, room));
                    }
                }
                best.map(|(index, _)| index)
            }
        };
        if self.alive.load(Ordering::SeqCst) == 0 {
            return Poll::Ready(None);
        }
        if let Some(index) = pick() {
            return Poll::Ready(Some(index));
        }
        self.loop_waker.register(cx.waker());
        self.loop_waiting.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if let Some(index) = pick() {
            self.loop_waiting.store(false, Ordering::SeqCst);
            return Poll::Ready(Some(index));
        }
        if self.alive.load(Ordering::SeqCst) == 0 {
            return Poll::Ready(None);
        }
        Poll::Pending
    }

    /// Wakes every worker, for them to find their rings abandoned once drained.
    fn close(&self) {
        for waker in &*self.wakers {
            waker.wake();
        }
    }
}

/// Counts a worker out when it ends, however it ends, and wakes the loop when the last one goes.
struct Alive<'a>(&'a Signals);

impl Drop for Alive<'_> {
    fn drop(&mut self) {
        if self.0.alive.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.loop_waker.wake();
        }
    }
}

/// One worker: drains its ring, parks when it is empty, exits once the loop has let go of it
/// and it is empty.
async fn work<Message, Body, Cx, State>(
    shared: Arc<Shared<Body, State, Cx>>,
    signals: Arc<Signals>,
    index: usize,
    mut ring: Consumer<Message>,
    spin: Spin,
    chunk: bool,
) where
    Message: IncomingMessage + Send,
    Body: Handler<Message, Cx, State>,
    Cx: BuildContext<Message> + Send + Sync + 'static,
    State: Send + Sync,
{
    let _alive = Alive(&signals);
    let mut encode = BytesMut::new();
    loop {
        if chunk {
            let available = ring.slots();
            if available > 0 {
                if let Ok(batch) = ring.read_chunk(available) {
                    for msg in batch {
                        shared.handle(msg, &mut encode).await;
                    }
                }
                signals.freed();
                continue;
            }
        } else if let Ok(msg) = ring.pop() {
            // Freed before the handler runs, so the loop refills the ring meanwhile.
            signals.freed();
            shared.handle(msg, &mut encode).await;
            continue;
        }
        if ring.is_abandoned() {
            if ring.is_empty() {
                break;
            }
            continue;
        }
        // The closures own a unique borrow of the ring: a shared one would make this future
        // `!Send`, the consumer being `!Sync`.
        let waiting = &mut ring;
        if spin.wait(move || !waiting.is_empty()).await {
            continue;
        }
        let waiting = &mut ring;
        let signals = &*signals;
        poll_fn(move |cx| {
            signals.wakers[index].register(cx.waker());
            signals.parked[index].store(true, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            if !waiting.is_empty() || waiting.is_abandoned() {
                signals.parked[index].store(false, Ordering::SeqCst);
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
    }
}

/// The loop of a pool fed through a ring per worker.
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
    let keyed = workers.by_key;
    let signals = Arc::new(Signals::new(count));
    let mut rings = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let (producer, consumer) = RingBuffer::new(knobs.ring);
        rings.push(producer);
        let shared = Arc::clone(&shared);
        let signals = Arc::clone(&signals);
        let (spin, chunk) = (knobs.spin, knobs.chunk);
        crew.0.push(placement.start(index, move || {
            work(shared, signals, index, consumer, spin, chunk)
        }));
    }
    let name = &shared.name;
    let mut stream = pin!(subscriber.stream());
    let mut cancelled = pin!(shutdown.cancelled());
    // The next ring in turn: the pool's round-robin, and where keyless deliveries of the lanes go.
    let mut turn = 0usize;
    'pull: loop {
        if shutdown.is_cancelled() {
            break;
        }
        // A pool picks its ring before it pulls, so nothing comes off the stream that no ring
        // could take; a lane learns its ring from the delivery.
        let picked = if keyed {
            None
        } else {
            let which = if knobs.least {
                Which::Most(turn)
            } else {
                Which::One(turn)
            };
            // The closure owns a unique borrow of the producers: a shared one would make the loop
            // `!Send`, a producer being `!Sync`.
            let (producers, waiting, signals) = (&mut rings, &mut cancelled, &*signals);
            let room = poll_fn(move |cx| match signals.poll_room(producers, which, cx) {
                Poll::Ready(Some(index)) => Poll::Ready(Turn::Delivery(index)),
                Poll::Ready(None) => Poll::Ready(Turn::Ended),
                Poll::Pending => waiting.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match room {
                Turn::Delivery(index) => Some(index),
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
        };
        let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match pulled {
            Turn::Delivery(Ok(msg)) => {
                let index = picked
                    .unwrap_or_else(|| msg.partition_key().map_or(turn, |key| lane_of(key, count)));
                if picked.is_some() || msg.partition_key().is_none() {
                    turn = (index + 1) % count;
                }
                if keyed {
                    // A lane waits for its own ring, whatever else has room: a key stays in
                    // order. Shutdown does not cut the wait short, so the delivery in hand is
                    // handed over rather than dropped.
                    let (producers, signals) = (&mut rings, &*signals);
                    let room =
                        poll_fn(move |cx| signals.poll_room(producers, Which::One(index), cx))
                            .await;
                    if room.is_none() {
                        error!(
                            target: "ruststream::dispatch",
                            subscription = %name,
                            "every worker terminated; stopping dispatch",
                        );
                        break 'pull;
                    }
                }
                // The ring had room, and only the loop fills it.
                if rings[index].push(msg).is_err() {
                    unreachable!("the loop pushes only after it saw room");
                }
                signals.pushed(index);
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
    // Letting go of the rings tells each worker to drain what its ring holds and exit: what was
    // pulled off the stream is handled and settled unless the shutdown timeout aborts the loop.
    drop(rings);
    signals.close();
    crew.join().await;
}
