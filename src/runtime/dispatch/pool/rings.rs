//! A bounded SPSC ring per worker: the loop pushes each delivery to one ring (round-robin, the ring
//! with the most room, or the ring a key hashes to) and wakes that worker only if it is parked; a
//! worker drains its ring and parks only when it finds it empty.
//!
//! Per delivery, under load: a plain store to push and one to pop (`rtrb`, a preallocated ring
//! whose sides read each other's index only when their cached copy runs out, so a worker reads the
//! loop's index once per burst it drains), and one `SeqCst` fence on each side for the park and
//! room handshakes; no allocation, no wake. Round-robin puts up to half a ring into the next ring
//! in turn that has room, skipping full ones, and parks only when every ring is full; the first
//! ring to drain to half wakes it, so a saturated pool wakes its loop about once per half ring
//! rather than once per delivery, and no worker idles while the loop waits on another's ring.
//! Picking the ring with the most room reads every worker's index, `n` cross-core loads per
//! delivery; round-robin reads a ring's index only when its cached copy says the ring is full. Read-ahead: per ring, one delivery in process plus
//! the ring's capacity. A key always goes to the same ring, and a ring is drained in order by one
//! worker, so per-key order holds; the loop waits on a full ring rather than reorder a key.
//!
//! A producer is `Send` and not `Sync`: the loop's task owns them all, and when a multi-threaded
//! runtime moves that task to another thread, the scheduler's hand-off orders everything the task
//! did before the move before everything it does after, so the producer's unsynchronized cached
//! index stays one thread's at a time.

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

use super::placement::{Member, start_thread};
use super::research::{Knobs, Spin};
use super::{Crew, Placement, Shared};
use crate::runtime::dispatch::{Handler, LocalHandler, Shutdown, Turn, Workers, lane_of};
use crate::{BuildContext, IncomingMessage, Subscriber};

/// What `loop_waiting` holds while the loop waits for room in any ring.
const ANY: usize = usize::MAX;

/// Who waits on whom: a worker on its empty ring, the loop on room.
struct Signals {
    parked: Box<[CachePadded<AtomicBool>]>,
    wakers: Box<[CachePadded<AtomicWaker>]>,
    /// 0 while the loop pulls; the waited ring's index plus one, or [`ANY`], while it waits.
    loop_waiting: CachePadded<AtomicUsize>,
    loop_waker: AtomicWaker,
    /// A ring wakes a loop waiting on any ring once it holds no more than this; a loop waiting on
    /// one ring (a lane's) is woken as soon as that ring has room.
    low_water: usize,
    capacity: usize,
    /// The workers still running; the last one out wakes the loop.
    alive: AtomicUsize,
}

/// Which ring the loop waits on for room.
#[derive(Clone, Copy)]
enum Which {
    /// This one: the one a key hashes to.
    One(usize),
    /// The first with room from here on, in turn.
    Next(usize),
    /// Whichever has the most room, ties going to the first from here.
    Most(usize),
}

impl Signals {
    fn new(count: usize, capacity: usize) -> Self {
        Self {
            parked: (0..count)
                .map(|_| CachePadded::new(AtomicBool::new(false)))
                .collect(),
            wakers: (0..count)
                .map(|_| CachePadded::new(AtomicWaker::new()))
                .collect(),
            loop_waiting: CachePadded::new(AtomicUsize::new(0)),
            loop_waker: AtomicWaker::new(),
            low_water: capacity / 2,
            capacity,
            alive: AtomicUsize::new(count),
        }
    }

    /// Worker `index` freed a slot of its ring, which now holds `held()`: wakes the loop if it
    /// waits for this ring and it has room, or for any ring and this one drained to low water.
    fn freed(&self, index: usize, held: impl FnOnce() -> usize) {
        // Orders the ring's index store before the flag's load, against the loop's flag store
        // before its ring load.
        fence(Ordering::SeqCst);
        let waiting = self.loop_waiting.load(Ordering::Relaxed);
        let wanted = match waiting {
            0 => return,
            ANY => self.low_water,
            one if one == index + 1 => self.capacity - 1,
            _ => return,
        };
        if held() <= wanted
            && self
                .loop_waiting
                .compare_exchange(waiting, 0, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
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
            Which::Next(start) => {
                let count = rings.len();
                (0..count)
                    .map(|offset| (start + offset) % count)
                    .find(|&index| !rings[index].is_full())
            }
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
        let waiting = match which {
            Which::One(index) => index + 1,
            Which::Next(_) | Which::Most(_) => ANY,
        };
        self.loop_waiting.store(waiting, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if let Some(index) = pick() {
            self.loop_waiting.store(0, Ordering::SeqCst);
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
                signals.freed(index, || ring.slots());
                continue;
            }
        } else if let Ok(msg) = ring.pop() {
            // Freed before the handler runs, so the loop refills the ring meanwhile.
            signals.freed(index, || ring.slots());
            shared.handle(msg, &mut encode).await;
            continue;
        }
        if ring.is_abandoned() {
            // The abandonment is read without ordering; this orders the loop's last pushes
            // before the emptiness check below.
            fence(Ordering::Acquire);
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

/// [`work`] for a [`LocalHandler`]: the same loop, its future built and polled on one thread.
// The `!Send` path is a prototype reached from a unit test only, not from a registration.
#[cfg_attr(not(test), allow(dead_code))]
async fn work_local<Message, Body, Cx, State>(
    shared: Arc<Shared<Body, State, Cx>>,
    signals: Arc<Signals>,
    index: usize,
    mut ring: Consumer<Message>,
    spin: Spin,
    chunk: bool,
) where
    Message: IncomingMessage + Send,
    Body: LocalHandler<Message, Cx, State>,
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
                        shared.handle_local(msg, &mut encode).await;
                    }
                }
                signals.freed(index, || ring.slots());
                continue;
            }
        } else if let Ok(msg) = ring.pop() {
            // Freed before the handler runs, so the loop refills the ring meanwhile.
            signals.freed(index, || ring.slots());
            shared.handle_local(msg, &mut encode).await;
            continue;
        }
        if ring.is_abandoned() {
            // The abandonment is read without ordering; this orders the loop's last pushes
            // before the emptiness check below.
            fence(Ordering::Acquire);
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
    subscriber: Sub,
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
    let (spin, chunk) = (knobs.spin, knobs.chunk);
    let start = |index, consumer, signals| {
        let shared = Arc::clone(&shared);
        placement.start(index, move || {
            work(shared, signals, index, consumer, spin, chunk)
        })
    };
    feed(subscriber, &shared.name, shutdown, workers, knobs, start).await;
}

/// Research (#417): the loop of a pool of dedicated threads whose handler's future need not be
/// `Send`: each worker's future is built on its thread and polled there alone.
// The `!Send` path is a prototype reached from a unit test only, not from a registration.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) async fn run_local<Sub, Body, Cx, State>(
    subscriber: Sub,
    shared: Arc<Shared<Body, State, Cx>>,
    shutdown: Shutdown,
    workers: Workers,
    knobs: Knobs,
) where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Body: LocalHandler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let (spin, chunk) = (knobs.spin, knobs.chunk);
    let start = |index, consumer, signals| {
        let shared = Arc::clone(&shared);
        start_thread(index, move || {
            work_local(shared, signals, index, consumer, spin, chunk)
        })
    };
    feed(subscriber, &shared.name, shutdown, workers, knobs, start).await;
}

/// The loop over `subscriber`, with a ring per worker that `start` starts.
async fn feed<Sub, Start>(
    mut subscriber: Sub,
    name: &str,
    shutdown: Shutdown,
    workers: Workers,
    knobs: Knobs,
    mut start: Start,
) where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Start: FnMut(usize, Consumer<Sub::Message>, Arc<Signals>) -> Member,
{
    let count = workers.count;
    let keyed = workers.by_key;
    let signals = Arc::new(Signals::new(count, knobs.ring));
    let batch = knobs.batch();
    let mut rings = Vec::with_capacity(count);
    let mut crew = Crew(Vec::with_capacity(count));
    for index in 0..count {
        let (producer, consumer) = RingBuffer::new(knobs.ring);
        rings.push(producer);
        crew.0.push(start(index, consumer, Arc::clone(&signals)));
    }
    let mut stream = pin!(subscriber.stream());
    let mut cancelled = pin!(shutdown.cancelled());
    // The next ring in turn: the pool's round-robin, and where keyless deliveries of the lanes go;
    // and how many deliveries the pool has put in it this turn.
    let mut turn = 0usize;
    let mut in_turn = 0usize;
    let mut pushed = 0u64;
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
                Which::Next(turn)
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
                if picked.is_some() && !knobs.least {
                    // A turn puts up to `batch` deliveries in its ring, or fills it; a full ring
                    // skipped on the way ends its turn.
                    if index != turn {
                        in_turn = 0;
                    }
                    in_turn += 1;
                    turn = index;
                    if in_turn >= batch || rings[index].is_full() {
                        turn = (index + 1) % count;
                        in_turn = 0;
                    }
                } else if picked.is_some() || msg.partition_key().is_none() {
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
                pushed += 1;
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
    let queued: usize = rings
        .iter()
        .map(|ring| ring.buffer().capacity() - ring.slots())
        .sum();
    drop(rings);
    signals.close();
    crew.join().await;
    if std::env::var_os("RUSTSTREAM_RESEARCH_TRACE").is_some() {
        eprintln!("rings loop: pushed {pushed}, queued at close {queued}");
    }
}
