//! The ring hand-off of a `threads(n)` subscription: deliveries handled on `n` threads of their
//! own, each fed through one bounded ring, whatever runtime the threads run.
//!
//! The subscription's loop ([`Feed`]) reads the stream. It spreads what it reads over the rings
//! round-robin, skipping a full ring to the next one with room, and stops polling only when every
//! ring is full; `by_key` hashes the key onto a fixed ring and waits on that one, so a key keeps
//! its order. A thread's [`Drain`] drains its ring and parks only when it has found it empty for a
//! few microseconds of spinning; the loop wakes it only when it is flagged parked, so under load a
//! delivery costs no wake, and behind a handler lighter than the loop's read few do.
//!
//! A loop that waits on room is woken by a ring that has drained to half, not by every pop: the
//! first ring to do so when every ring is full, the key's own ring under `by_key`. A hot key behind
//! handlers slower than the loop thus costs one wake per half ring of deliveries. Shutdown and a
//! thread that leaves wake the loop at once; stopping, a loop that holds a key's delivery hands it
//! to that ring's first free slot. How the deliveries spread over the threads is the service's
//! choice: `by_key` partitions by the key the service picks, and a key that keeps one ring full
//! while the other threads idle is the consequence of that choice, as with partitioning anywhere.
//!
//! Per delivery, under load: `rtrb`'s push and pop (a store each, the other side's index read only
//! when the cached copy runs out; the ring is preallocated by [`open`], so nothing allocates), a
//! `SeqCst` fence on each side for the park and room handshakes, and a load of the ring's
//! reference count that tells the loop the thread is still there. The read-ahead is bounded by
//! `n` times (1 + ring size): one delivery in hand per thread plus a full ring each.
//!
//! The runner starts the threads and runs [`Drain::work`] on each, and runs [`Feed::read`] in the
//! subscription's loop. A thread that never waits in its handler never yields to its own runtime:
//! the runner's handler yields cooperatively after a run of deliveries, so a continuation, a hook
//! or a retry timer on that runtime does not wait for a whole burst.
//!
//! A producer is `Send` and not `Sync`: the loop's task owns every producer, and when a
//! multi-threaded runtime moves that task to another thread, the scheduler's hand-off orders
//! everything the task did before the move before everything it does after, so a producer's
//! unsynchronized cached index stays one thread's at a time.

use std::boxed::Box;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::poll_fn;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::spin_loop;
use std::iter::repeat_with;
use std::ops::Deref;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use std::vec::Vec;

use futures::Stream;
use futures::task::AtomicWaker;
use rtrb::{Consumer, Producer, PushError, RingBuffer};

use super::Turn;
use crate::host::{Halt, ShutdownWait};
use ruststream_core::diag::error;

/// The entries of each thread's ring: enough that a thread finishing a delivery finds the next
/// one waiting while the loop reads on, few enough that the read-ahead stays small.
pub const RING: usize = 8;

/// Deliveries the feed hands over in a row before it gives the executor a turn.
const TURN_LENGTH: u32 = 128;

/// How long a thread that found its ring empty keeps looking before it parks. A light handler
/// drains its ring faster than the loop fills it; parked on its runtime's driver between
/// deliveries, the thread would cost the loop a system call to wake it for most of them. Within
/// this window the next delivery usually comes first. It is also the CPU a thread spends after
/// the last delivery of a burst.
const SPIN: Duration = Duration::from_micros(10);

/// The spins between two looks at an empty ring, at their longest.
const LONGEST_GAP: u32 = 64;

/// The thread a partition `key` belongs to among `threads`: the same key always maps to the same one.
///
/// `threads` is not zero.
#[must_use]
pub fn key_index(key: &[u8], threads: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    // The modulo keeps the value below `threads`, which is a usize.
    #[allow(clippy::cast_possible_truncation)]
    {
        (hasher.finish() % threads as u64) as usize
    }
}

/// A value on a cache line of its own, so what one side writes does not evict what the other
/// reads.
#[repr(align(128))]
struct Padded<T>(T);

impl<T> Deref for Padded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// What `loop_waiting` holds while the loop waits for room in any ring.
const ANY: usize = usize::MAX;

/// What `loop_waiting` sets beside a ring's index while the loop, stopping, waits for that ring's
/// first free slot.
const STOPPING: usize = 1 << (usize::BITS - 1);

/// Who waits on whom: a thread on its empty ring, the loop on room.
struct Signals {
    parked: Box<[Padded<AtomicBool>]>,
    wakers: Box<[Padded<AtomicWaker>]>,
    /// 0 while the loop reads; while it waits, [`ANY`] or the waited ring's index plus one, with
    /// [`STOPPING`] set where the ring's first free slot will do.
    loop_waiting: Padded<AtomicUsize>,
    loop_waker: AtomicWaker,
}

/// Which ring the loop waits on for room.
#[derive(Clone, Copy)]
enum Which {
    /// This one, the thread a key hashes to, until it has drained to half.
    One(usize),
    /// This one, at its first free slot: shutdown came, and the delivery in hand goes before the
    /// loop stops.
    Stopping(usize),
    /// The first with room from here on, in turn.
    Next(usize),
}

/// Why the loop could not hand a delivery on.
enum Stuck {
    /// The thread behind a ring is gone.
    Gone(usize),
    /// Shutdown came first.
    Shutdown,
}

impl Signals {
    fn new(count: usize) -> Self {
        Self {
            parked: repeat_with(|| Padded(AtomicBool::new(false)))
                .take(count)
                .collect(),
            wakers: repeat_with(|| Padded(AtomicWaker::new()))
                .take(count)
                .collect(),
            loop_waiting: Padded(AtomicUsize::new(0)),
            loop_waker: AtomicWaker::new(),
        }
    }

    /// Thread `index` freed a slot of its ring, which now holds `held` deliveries: wakes the loop
    /// if it waits on this ring or on any ring and this one drained to half, or if it is stopping
    /// and waits on this ring at all.
    #[inline]
    fn freed(&self, index: usize, held: usize) {
        // Orders the ring's index store before the flag's load, against the loop's flag store
        // before its ring load.
        fence(Ordering::SeqCst);
        let waiting = self.loop_waiting.load(Ordering::Relaxed);
        let wakes = match waiting {
            0 => false,
            ANY => held <= RING / 2,
            one if one == index + 1 => held <= RING / 2,
            other => other == STOPPING | (index + 1),
        };
        if wakes
            && self
                .loop_waiting
                .compare_exchange(waiting, 0, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
        {
            self.loop_waker.wake();
        }
    }

    /// The loop pushed to ring `index`: wakes its thread if it is parked.
    #[inline]
    fn pushed(&self, index: usize) {
        // Orders the ring's index store before the parked flag's load, against the thread's flag
        // store before its ring load.
        fence(Ordering::SeqCst);
        if self.parked[index].load(Ordering::Relaxed)
            && self.parked[index].swap(false, Ordering::SeqCst)
        {
            self.wakers[index].wake();
        }
    }

    /// Wakes the loop, whatever it waits on: a thread is leaving.
    fn leaving(&self) {
        self.loop_waiting.store(0, Ordering::SeqCst);
        self.loop_waker.wake();
    }

    /// The ring with room the loop waits for, by `which`; an error once the ring it would pick is
    /// abandoned by its thread.
    #[inline]
    fn poll_room<Item>(
        &self,
        rings: &[Producer<Item>],
        which: Which,
        cx: &TaskContext<'_>,
    ) -> Poll<Result<usize, usize>> {
        // A ring's thread is checked before the ring is picked: a delivery pushed to a ring nobody
        // drains would be lost.
        let usable = |index: usize| {
            let ring: &Producer<Item> = &rings[index];
            if ring.is_abandoned() {
                Some(Err(index))
            } else {
                (!ring.is_full()).then_some(Ok(index))
            }
        };
        let pick = || match which {
            Which::One(index) | Which::Stopping(index) => usable(index),
            Which::Next(start) => {
                let count = rings.len();
                (0..count).find_map(|offset| usable((start + offset) % count))
            }
        };
        if let Some(found) = pick() {
            return Poll::Ready(found);
        }
        self.loop_waker.register(cx.waker());
        let waiting = match which {
            Which::One(index) => index + 1,
            Which::Stopping(index) => STOPPING | (index + 1),
            Which::Next(_) => ANY,
        };
        self.loop_waiting.store(waiting, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if let Some(found) = pick() {
            self.loop_waiting.store(0, Ordering::SeqCst);
            return Poll::Ready(found);
        }
        Poll::Pending
    }

    /// Wakes every thread, for each to find its ring abandoned once it has drained it.
    fn close(&self) {
        for waker in &*self.wakers {
            waker.wake();
        }
    }
}

/// Wakes the loop when a thread's work ends, however it ends: a ring the loop waits on is then
/// found abandoned rather than waited on forever.
struct Leaving<'a>(&'a Signals);

impl Drop for Leaving<'_> {
    fn drop(&mut self) {
        self.0.leaving();
    }
}

/// One thread's end of a ring: it drains the ring through a handler and ends once the loop has
/// let go of the ring and it is empty.
pub struct Drain<Item> {
    signals: Arc<Signals>,
    index: usize,
    ring: Consumer<Item>,
}

impl<Item> Debug for Drain<Item> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Drain")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl<Item> Drain<Item> {
    /// Drains the ring through `handle`, parks when it has stayed empty for [`SPIN`], and returns
    /// once the loop has let go of the ring and it is empty.
    ///
    /// A thread whose ring never stays empty never parks, and a handler that completes without
    /// waiting never yields, so the runner's `handle` yields to its runtime now and then (tokio's
    /// `consume_budget`): without it an `and_after` continuation, an `after(..)` hook or a
    /// `retry_after` timer on that runtime would wait for the whole burst.
    // The future is built on its thread and polled there alone, so it need not be `Send`: the
    // handler behind it only is.
    #[allow(clippy::future_not_send)]
    pub async fn work<Handle>(self, mut handle: Handle)
    where
        Handle: AsyncFnMut(Item),
    {
        let Self {
            signals,
            index,
            ring,
        } = self;
        let signals = &*signals;
        let _leaving = Leaving(signals);
        // Declared after the guard, so it drops first: the loop the guard wakes finds the ring
        // abandoned already.
        let mut ring = ring;
        loop {
            if let Ok(item) = ring.pop() {
                // Freed before the handler runs, so the loop refills the ring meanwhile.
                signals.freed(index, ring.slots());
                handle(item).await;
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
            if arrives(&ring) {
                continue;
            }
            let waiting = &ring;
            poll_fn(|cx| {
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
}

/// Waits up to [`SPIN`] on the empty ring for a delivery, or for the loop to let go of it: whether
/// either came first. The first looks come a few spins apart, so a delivery pushed right behind
/// the last one is taken at once; the clock is read only once the gap between looks has grown to
/// its longest.
fn arrives<Item>(ring: &Consumer<Item>) -> bool {
    let start = Instant::now();
    let mut spins = 1;
    loop {
        for _ in 0..spins {
            spin_loop();
        }
        if !ring.is_empty() || ring.is_abandoned() {
            return true;
        }
        if spins < LONGEST_GAP {
            spins *= 2;
        } else if start.elapsed() >= SPIN {
            return false;
        }
    }
}

/// The loop's end of the rings: it reads a subscription's stream and hands each delivery to a
/// thread's [`Drain`].
pub struct Feed<Item> {
    rings: Vec<Producer<Item>>,
    signals: Arc<Signals>,
    keyed: bool,
}

impl<Item> Debug for Feed<Item> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Feed")
            .field("rings", &self.rings.len())
            .field("keyed", &self.keyed)
            .finish_non_exhaustive()
    }
}

/// Opens `count` rings for a subscription, preallocated, and returns the loop's [`Feed`].
///
/// `start` is handed each ring's [`Drain`] in ring order, to run on a thread of its own. `keyed`
/// hashes each delivery's partition key onto a fixed ring.
///
/// # Errors
///
/// The first error `start` returns, for a thread that could not start; the threads handed so far
/// find their rings abandoned and end.
pub fn open<Item, Failure>(
    count: usize,
    keyed: bool,
    mut start: impl FnMut(Drain<Item>) -> Result<(), Failure>,
) -> Result<Feed<Item>, Failure> {
    let signals = Arc::new(Signals::new(count));
    let mut rings = Vec::with_capacity(count);
    for index in 0..count {
        let (producer, consumer) = RingBuffer::new(RING);
        let started = start(Drain {
            signals: Arc::clone(&signals),
            index,
            ring: consumer,
        });
        rings.push(producer);
        if let Err(failure) = started {
            // A thread already parked on its empty ring is woken to find it abandoned.
            Feed {
                rings,
                signals,
                keyed,
            }
            .let_go();
            return Err(failure);
        }
    }
    Ok(Feed {
        rings,
        signals,
        keyed,
    })
}

impl<Item> Feed<Item> {
    /// Letting go of the rings tells each thread to drain what its ring holds and end: what was
    /// read off the stream is handled and settled unless the loop is aborted first.
    pub fn let_go(&mut self) {
        self.rings.clear();
        self.signals.close();
    }

    /// Reads `stream` into the rings until it ends, `shutdown` fires, or a thread is gone.
    pub async fn read<Items, Failure, Stop>(
        &mut self,
        stream: Items,
        name: &str,
        shutdown: &Stop,
        key: fn(&Item) -> Option<&[u8]>,
    ) where
        Items: Stream<Item = Result<Item, Failure>>,
        Failure: Display,
        Stop: ShutdownWait + Halt,
    {
        let count = self.rings.len();
        let mut stream = pin!(stream);
        let mut cancelled = pin!(shutdown.cancelled());
        // The next ring in turn: the pool's round-robin, and where keyless deliveries of the
        // threads go.
        let mut turn = 0usize;
        // Deliveries handed over since the executor last had a turn.
        let mut handed = 0u32;
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            // A pool picks its ring before it reads, so nothing comes off the stream that no ring
            // could take; a thread learns its ring from the delivery.
            let picked = if self.keyed {
                None
            } else {
                let room = self.room(Which::Next(turn), Some(cancelled.as_mut())).await;
                match room {
                    Ok(index) => Some(index),
                    Err(stuck) => {
                        report(name, &stuck);
                        break;
                    }
                }
            };
            let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
                Poll::Ready(None) => Poll::Ready(Turn::Ended),
                Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match pulled {
                Turn::Delivery(Ok(item)) => {
                    let keyed = if picked.is_some() { None } else { key(&item) };
                    let index =
                        picked.unwrap_or_else(|| keyed.map_or(turn, |key| key_index(key, count)));
                    if keyed.is_none() {
                        turn = (index + 1) % count;
                    }
                    // A thread's ring may be gone before the delivery is handed to it.
                    if picked.is_none() && self.rings[index].is_abandoned() {
                        report(name, &Stuck::Gone(index));
                        break;
                    }
                    // A pool has picked a ring with room already, so its push goes through. A full
                    // ring is waited on, whatever else has room: a key stays in order.
                    match self.rings[index].push(item) {
                        Ok(()) => {
                            self.signals.pushed(index);
                            // Rings with room and a ready stream never make this loop wait, so
                            // a burst would hold the executor until it ended.
                            handed += 1;
                            if handed >= TURN_LENGTH {
                                handed = 0;
                                yield_once().await;
                            }
                        }
                        Err(PushError::Full(item)) => {
                            let handed_over =
                                self.hand_over_when_room(index, item, cancelled.as_mut());
                            if let Err(stuck) = handed_over.await {
                                report(name, &stuck);
                                break;
                            }
                        }
                    }
                }
                Turn::Delivery(Err(err)) => {
                    error!(
                        target: "ruststream::dispatch",
                        subscription = %name,
                        error = %err,
                        "subscriber stream error",
                    );
                    // A broker that keeps answering with errors fills no ring, so nothing else
                    // here would hand the executor a turn.
                    yield_once().await;
                }
                Turn::Ended => {
                    shutdown.stream_ended(name);
                    break;
                }
                Turn::Shutdown => break,
            }
        }
    }

    /// Pushes `item` to ring `index` once the ring has drained to half. Shutdown wakes the wait
    /// without cutting it short: the ring's first free slot takes the delivery in hand from then
    /// on, so it is handed over rather than dropped, and the loop stops as soon as it can.
    #[cold]
    async fn hand_over_when_room<Cancelled>(
        &mut self,
        index: usize,
        mut item: Item,
        cancelled: Pin<&mut Cancelled>,
    ) -> Result<(), Stuck>
    where
        Cancelled: Future<Output = ()>,
    {
        let mut which = Which::One(index);
        let mut cancelled = Some(cancelled);
        loop {
            match self.rings[index].push(item) {
                Ok(()) => {
                    self.signals.pushed(index);
                    return Ok(());
                }
                Err(PushError::Full(back)) => item = back,
            }
            match self.room(which, cancelled.as_mut().map(Pin::as_mut)).await {
                Ok(_) => {}
                Err(Stuck::Shutdown) => {
                    // The wait has resolved, and is not polled again.
                    cancelled = None;
                    which = Which::Stopping(index);
                }
                Err(gone) => return Err(gone),
            }
        }
    }

    /// Waits for room by `which`, or for shutdown where `cancelled` is given.
    #[inline]
    async fn room<Cancelled>(
        &mut self,
        which: Which,
        mut cancelled: Option<Pin<&mut Cancelled>>,
    ) -> Result<usize, Stuck>
    where
        Cancelled: Future<Output = ()>,
    {
        let (rings, signals) = (&mut self.rings, &*self.signals);
        poll_fn(move |cx| match signals.poll_room(rings, which, cx) {
            Poll::Ready(Ok(index)) => Poll::Ready(Ok(index)),
            Poll::Ready(Err(index)) => Poll::Ready(Err(Stuck::Gone(index))),
            Poll::Pending => cancelled.as_mut().map_or(Poll::Pending, |cancelled| {
                cancelled.as_mut().poll(cx).map(|()| Err(Stuck::Shutdown))
            }),
        })
        .await
    }
}

/// Completes on its second poll, having woken its task: the executor runs its other tasks first.
async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Logs why the loop stopped handing deliveries on, where it is not shutdown.
fn report(name: &str, stuck: &Stuck) {
    if let Stuck::Gone(thread) = stuck {
        // A thread only ends early if its work panicked outside the handler; stop reading rather
        // than hand it deliveries nobody handles.
        error!(
            target: "ruststream::dispatch",
            subscription = %name,
            thread,
            "dedicated thread terminated; stopping dispatch",
        );
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::format;
    use std::sync::Mutex;
    use std::task::{Wake, Waker};
    use std::thread;

    use futures::StreamExt;
    use futures::channel::mpsc::{self, UnboundedSender};
    use futures::executor::block_on;
    use futures::stream;
    use tokio::sync::{Notify, Semaphore};
    use tokio::task::{JoinHandle, yield_now};
    use tokio::time;
    use tokio::time::error::Elapsed;
    use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

    use super::*;

    /// The deliveries the loop may hold ahead of the handlers: one in hand per thread and a full
    /// ring each.
    const READ_AHEAD: usize = 2 * (1 + RING);

    /// A shutdown signal for the tests.
    #[derive(Default)]
    struct Stop(CancellationToken);

    impl Stop {
        fn raise(&self) {
            self.0.cancel();
        }
    }

    impl ShutdownWait for Stop {
        type Wait<'a> = WaitForCancellationFuture<'a>;

        fn is_cancelled(&self) -> bool {
            self.0.is_cancelled()
        }

        fn cancelled(&self) -> WaitForCancellationFuture<'_> {
            self.0.cancelled()
        }
    }

    impl Halt for Stop {
        fn fail(&self, _subscription: &str, _reason: &str) {
            self.raise();
        }

        fn stream_ended(&self, _subscription: &str) {}
    }

    /// Opens `count` rings and runs each thread's work on a thread of its own.
    fn start<Item, Each, Handle>(
        count: usize,
        keyed: bool,
        mut each: Each,
    ) -> (Feed<Item>, Vec<thread::JoinHandle<()>>)
    where
        Item: Send + 'static,
        Each: FnMut() -> Handle,
        Handle: AsyncFnMut(Item) + Send + 'static,
    {
        let mut threads = Vec::with_capacity(count);
        let feed = open(count, keyed, |drain| {
            let handle = each();
            threads.push(thread::spawn(move || block_on(drain.work(handle))));
            Ok::<_, Infallible>(())
        })
        .expect("the threads start");
        (feed, threads)
    }

    /// With every handler held, the loop reads no more than the rings and the threads can hold;
    /// on shutdown the threads handle every delivery it read, none is dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_read_ahead_is_bounded_and_drained_on_shutdown() {
        let gate = Arc::new(Semaphore::new(0));
        let handled = Arc::new(AtomicUsize::new(0));
        let pulled = Arc::new(AtomicUsize::new(0));
        let full = Arc::new(Notify::new());
        let (feed, threads) = start(2, false, || {
            let (gate, handled) = (Arc::clone(&gate), Arc::clone(&handled));
            async move |_item: u32| {
                gate.acquire().await.expect("the gate stays open").forget();
                handled.fetch_add(1, Ordering::SeqCst);
            }
        });
        let items = {
            let (pulled, full) = (Arc::clone(&pulled), Arc::clone(&full));
            stream::iter(0..100u32).map(move |item| {
                // Both rings full: the loop cannot have read less before it waits for room.
                if pulled.fetch_add(1, Ordering::SeqCst) + 1 == 2 * RING {
                    full.notify_one();
                }
                Ok::<_, Infallible>(item)
            })
        };
        let stop = Arc::new(Stop::default());
        let stopping = Arc::clone(&stop);
        let reading = tokio::spawn(async move {
            let mut feed = feed;
            feed.read(items, "bounded", &*stopping, |_| None).await;
            feed
        });
        full.notified().await;
        stop.raise();
        let mut feed = reading.await.expect("the loop stops reading");
        // The rings go while every thread still holds its first delivery: what they hold must
        // be handled all the same.
        feed.let_go();
        gate.add_permits(100);
        for thread in threads {
            thread.join().expect("the thread ends cleanly");
        }
        let pulled = pulled.load(Ordering::SeqCst);
        assert!(pulled <= READ_AHEAD, "read {pulled} ahead of held handlers");
        assert_eq!(
            handled.load(Ordering::SeqCst),
            pulled,
            "a delivery read was not handled"
        );
    }

    /// A loop of one thread brought to a wait on its full ring: the thread holds the first
    /// delivery at `gate`, the ring holds the next [`RING`], and a keyed loop holds one more.
    struct FullRing {
        reading: JoinHandle<Feed<u32>>,
        threads: Vec<thread::JoinHandle<()>>,
        signals: Arc<Signals>,
        stop: Arc<Stop>,
        gate: Arc<Semaphore>,
        handled: Arc<AtomicUsize>,
        /// Kept open, so the stream does not end under the loop.
        _deliveries: UnboundedSender<u32>,
    }

    impl FullRing {
        /// Starts the loop and returns once it waits on the ring; with `leaves`, the first
        /// delivery takes the thread down once `gate` lets it go.
        async fn start(keyed: bool, leaves: bool) -> Self {
            let gate = Arc::new(Semaphore::new(0));
            let handled = Arc::new(AtomicUsize::new(0));
            let holding = Arc::new(Notify::new());
            let (feed, threads) = start(1, keyed, || {
                let (gate, handled) = (Arc::clone(&gate), Arc::clone(&handled));
                let holding = Arc::clone(&holding);
                async move |item: u32| {
                    if item == 0 {
                        holding.notify_one();
                    }
                    gate.acquire().await.expect("the gate stays open").forget();
                    assert!(
                        !leaves || item > 0,
                        "the first delivery takes the thread down"
                    );
                    handled.fetch_add(1, Ordering::SeqCst);
                }
            });
            let signals = Arc::clone(&feed.signals);
            let (deliveries, items) = mpsc::unbounded();
            let stop = Arc::new(Stop::default());
            let stopping = Arc::clone(&stop);
            let reading = tokio::spawn(async move {
                let mut feed = feed;
                let items = items.map(Ok::<_, Infallible>);
                feed.read(items, "full", &*stopping, |_| Some(b"key".as_slice()))
                    .await;
                feed
            });
            // The thread takes the first delivery before the rest arrive, so the ring fills
            // behind it.
            deliveries.unbounded_send(0).expect("the loop reads");
            holding.notified().await;
            for item in 1..=RING + 1 {
                let item = u32::try_from(item).expect("a few deliveries");
                deliveries.unbounded_send(item).expect("the loop reads");
            }
            let full = Self {
                reading,
                threads,
                signals,
                stop,
                gate,
                handled,
                _deliveries: deliveries,
            };
            full.waiting(|waiting| waiting != 0).await;
            full
        }

        /// Returns once the loop waits the way `waits` reads what it waits on.
        async fn waiting(&self, waits: impl Fn(usize) -> bool) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !waits(self.signals.loop_waiting.load(Ordering::SeqCst)) {
                assert!(
                    Instant::now() < deadline,
                    "the loop never came to the wait the test expects"
                );
                yield_now().await;
            }
        }

        /// The loop, once its read has returned; an error where it is still waiting.
        async fn stopped(&mut self) -> Result<Feed<u32>, Elapsed> {
            time::timeout(Duration::from_secs(10), &mut self.reading)
                .await
                .map(|read| read.expect("the loop ends cleanly"))
        }
    }

    /// A thread whose work ends early (a panic outside the handler's own catch) stops the loop
    /// rather than leaving it waiting on a ring nobody drains, a ring held above half included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_thread_that_dies_stops_the_loop() {
        for keyed in [false, true] {
            let mut full = FullRing::start(keyed, true).await;
            full.gate.add_permits(1);
            let mut feed = full
                .stopped()
                .await
                .unwrap_or_else(|_| panic!("the loop waited on a dead thread (keyed: {keyed})"));
            feed.let_go();
            for thread in full.threads {
                // The thread panicked on purpose.
                assert!(thread.join().is_err(), "the first delivery should panic");
            }
        }
    }

    /// Shutdown wakes a loop that waits for its key's ring to drain: the loop hands the delivery
    /// it holds to the ring's first free slot and stops, and every delivery it read is handled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_hands_a_waiting_key_its_first_free_slot() {
        let mut full = FullRing::start(true, false).await;
        full.stop.raise();
        // Stopping, the loop waits on its key's ring, the only one, for any free slot.
        full.waiting(|waiting| waiting == STOPPING | 1).await;
        // One delivery handled frees one slot, far from half the ring.
        full.gate.add_permits(1);
        let mut feed = full
            .stopped()
            .await
            .expect("shutdown left the loop waiting on its key's ring");
        feed.let_go();
        full.gate.add_permits(RING + 1);
        for thread in full.threads {
            thread.join().expect("the thread ends cleanly");
        }
        assert_eq!(
            full.handled.load(Ordering::SeqCst),
            RING + 2,
            "a delivery the loop read was not handled"
        );
    }

    /// A key keeps one thread, so its deliveries reach the handler in the order they were read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_key_keeps_its_order_on_its_thread() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (mut feed, threads) = start(3, true, || {
            let seen = Arc::clone(&seen);
            async move |item: u32| seen.lock().expect("the lock is not poisoned").push(item)
        });
        let items = stream::iter(0..200u32).map(Ok::<_, Infallible>);
        feed.read(items, "keyed", &Stop::default(), |_| {
            Some(b"one key".as_slice())
        })
        .await;
        feed.let_go();
        for thread in threads {
            thread.join().expect("the thread ends cleanly");
        }
        let seen = seen.lock().expect("the lock is not poisoned");
        assert_eq!(*seen, (0..200).collect::<Vec<_>>());
    }

    /// The deliveries of the hot key in [`a_hot_key_wakes_the_loop_once_per_half_ring`]: fewer
    /// than a turn, so the loop never wakes itself to give an executor its turn.
    const HOT_KEY: usize = 100;

    const _: () = assert!(HOT_KEY < TURN_LENGTH as usize);

    /// Wakes the thread that polls a future, and counts the wakes.
    struct Unparks {
        thread: thread::Thread,
        wakes: AtomicUsize,
    }

    impl Wake for Unparks {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    /// Polls `future` to its end on this thread, parked between polls, and returns how many
    /// times it was woken.
    fn woken_until_done(future: impl Future<Output = ()>) -> usize {
        let unparks = Arc::new(Unparks {
            thread: thread::current(),
            wakes: AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&unparks));
        let mut cx = TaskContext::from_waker(&waker);
        let mut future = pin!(future);
        while future.as_mut().poll(&mut cx).is_pending() {
            thread::park();
        }
        unparks.wakes.load(Ordering::SeqCst)
    }

    /// Under one key and handlers far slower than the loop, the loop waits on the key's ring
    /// nearly all the time: it is woken once that ring has drained to half, so at most once per
    /// half ring of deliveries.
    #[test]
    fn a_hot_key_wakes_the_loop_once_per_half_ring() {
        let (mut feed, threads) = start(2, true, || {
            async move |_item: usize| {
                // Computation that never waits, far longer than the loop's read of a delivery.
                let until = Instant::now() + Duration::from_micros(100);
                while Instant::now() < until {
                    spin_loop();
                }
            }
        });
        let items = stream::iter(0..HOT_KEY).map(Ok::<_, Infallible>);
        let stop = Stop::default();
        let wakes = woken_until_done(feed.read(items, "hot", &stop, |_| Some(b"hot".as_slice())));
        feed.let_go();
        for thread in threads {
            thread.join().expect("the thread ends cleanly");
        }
        let bound = HOT_KEY / (RING / 2);
        assert!(
            wakes <= bound,
            "the loop was woken {wakes} times for {HOT_KEY} deliveries of one key, more than once \
             per half ring ({bound})"
        );
    }

    /// A stream that answers every poll with a ready error still lets the other tasks of the loop's
    /// executor run, so shutdown can be raised and heard.
    #[tokio::test(flavor = "current_thread")]
    async fn a_stream_of_ready_errors_hands_the_executor_a_turn() {
        let (mut feed, threads) = start(1, false, || async move |_item: u32| {});
        let stop = Arc::new(Stop::default());
        let raising = Arc::clone(&stop);
        // Runs only when the reading task yields; the executor has one thread.
        let raiser = tokio::spawn(async move { raising.raise() });
        let items = stream::repeat_with(|| Err::<u32, _>("broker down"));
        feed.read(items, "failing", &*stop, |_| None).await;
        raiser.await.expect("the raiser ran");
        feed.let_go();
        for thread in threads {
            thread.join().expect("the thread ends cleanly");
        }
    }

    /// A burst of ready deliveries with room in the rings hands the executor a turn after
    /// [`TURN_LENGTH`] of them, so a signal or shutdown on the same executor is heard.
    #[test]
    fn a_burst_of_ready_deliveries_hands_the_executor_a_turn() {
        // More room than one turn: the rings alone would not stop the loop first.
        let threads_count = 20;
        assert!(
            threads_count * RING > 128,
            "the rings hold more than a turn"
        );
        let gate = Arc::new(Semaphore::new(0));
        let (mut feed, threads) = start(threads_count, false, || {
            let gate = Arc::clone(&gate);
            async move |_item: u32| {
                gate.acquire().await.expect("the gate stays open").forget();
            }
        });
        let pulled = Arc::new(AtomicUsize::new(0));
        let counting = Arc::clone(&pulled);
        let items = stream::repeat_with(move || {
            counting.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>(0u32)
        });
        let stop = Stop::default();
        let counter = Arc::new(Wakes::default());
        let waker = Waker::from(Arc::clone(&counter));
        {
            let mut read = pin!(feed.read(items, "burst", &stop, |_| None));
            let first = read.as_mut().poll(&mut TaskContext::from_waker(&waker));
            assert!(first.is_pending(), "the loop did not give the turn up");
        }
        assert_eq!(pulled.load(Ordering::SeqCst), 128);
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "the loop asked for a turn"
        );
        feed.let_go();
        gate.add_permits(1000);
        for thread in threads {
            thread.join().expect("the thread ends cleanly");
        }
    }

    /// Counts the times a task is woken.
    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A thread that cannot start refuses the subscription, and the threads already started end
    /// instead of waiting on a loop that will never run.
    #[test]
    fn a_thread_that_cannot_start_refuses_the_rings() {
        let mut started = Vec::new();
        let opened = open::<u32, _>(3, false, |drain| {
            if started.len() == 1 {
                return Err("no thread for ring 1");
            }
            started.push(thread::spawn(move || {
                block_on(drain.work(async move |_item: u32| {}));
            }));
            Ok(())
        });
        assert_eq!(opened.err(), Some("no thread for ring 1"));
        for thread in started {
            thread.join().expect("the started thread ends");
        }
    }

    /// A thread that already parked on its empty ring is woken when a later one cannot start, so
    /// it finds its ring abandoned and ends rather than sleeping with its guard forever.
    #[test]
    fn a_thread_parked_before_a_failed_start_is_woken() {
        let mut parked: Option<Arc<Signals>> = None;
        let mut started = Vec::new();
        let opened = open::<u32, _>(2, false, |drain| {
            if let Some(signals) = &parked {
                let deadline = Instant::now() + Duration::from_secs(10);
                while !signals.parked[0].load(Ordering::SeqCst) {
                    assert!(Instant::now() < deadline, "the first thread never parked");
                    thread::yield_now();
                }
                return Err("no thread for ring 1");
            }
            parked = Some(Arc::clone(&drain.signals));
            started.push(thread::spawn(move || {
                block_on(drain.work(async move |_item: u32| {}));
            }));
            Ok(())
        });
        assert_eq!(opened.err(), Some("no thread for ring 1"));
        for thread in started {
            thread.join().expect("the parked thread ends");
        }
    }

    /// The rings and the threads show in a log by what tells them apart.
    #[test]
    fn the_feed_and_a_drain_are_shown_with_their_counts() {
        let mut drains = Vec::new();
        let feed = open::<u32, _>(2, true, |drain| {
            drains.push(drain);
            Ok::<_, Infallible>(())
        })
        .expect("the rings open");

        let shown = format!("{feed:?}");
        assert!(
            shown.contains("rings: 2") && shown.contains("keyed: true"),
            "{shown}"
        );
        let shown = format!("{:?}", drains[1]);
        assert!(shown.contains("index: 1"), "{shown}");
    }

    /// The thread is below the thread count for any key, and one key always lands on one thread.
    #[test]
    fn a_key_index_is_below_the_count_and_stable() {
        for threads in 1..=7 {
            for key in [b"".as_slice(), b"a", b"orders-17", &[0xff; 64]] {
                let drain = key_index(key, threads);
                assert!(drain < threads);
                assert_eq!(drain, key_index(key, threads));
            }
        }
    }
}
