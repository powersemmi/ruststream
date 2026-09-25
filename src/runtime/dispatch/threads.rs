//! The dedicated threads behind `threads(n)`: a subscription's deliveries handled on `n` threads
//! of its own, each running a current-thread runtime, fed through one bounded ring per thread.
//!
//! The subscription's loop stays on the app's runtime and reads the stream. It spreads what it
//! reads over the rings round-robin, skipping a full ring to the next one with room, and stops
//! polling only when every ring is full; `by_key` hashes the key onto a fixed ring and waits on
//! that one, so a key keeps its order. A thread drains its ring and parks only when it finds it
//! empty; the loop wakes it only when it is flagged parked, so under load a delivery costs no
//! wake. A loop that waits on room is woken by the first ring to drain to half (or, waiting on a
//! lane, by the lane's first free slot), not by every pop.
//!
//! Per delivery, under load: `rtrb`'s push and pop (a store each, the other side's index read only
//! when the cached copy runs out; the ring is preallocated when the subscription starts, so
//! nothing allocates), a `SeqCst` fence on each side for the park and room handshakes, and a load
//! of the ring's reference count that tells the loop the thread is still there. The read-ahead is
//! bounded by `n` times (1 + ring size): one delivery in hand per thread plus a full ring each.
//!
//! A producer is `Send` and not `Sync`: the loop's task owns every producer, and when a
//! multi-threaded runtime moves that task to another thread, the scheduler's hand-off orders
//! everything the task did before the move before everything it does after, so a producer's
//! unsynchronized cached index stays one thread's at a time.

use std::fmt::Display;
use std::future::{Future, Pending, poll_fn};
use std::io;
use std::ops::Deref;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::task::{Context as TaskContext, Poll};
use std::thread;

use futures::Stream;
use futures::task::AtomicWaker;
use rtrb::{Consumer, Producer, RingBuffer};
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tracing::{debug, error};

use super::{Shutdown, Turn, lane_of};

/// The entries of each thread's ring: enough that a thread finishing a delivery finds the next
/// one waiting while the loop reads on, few enough that the read-ahead stays small.
pub(super) const RING: usize = 8;

/// A thread of a `threads(n)` subscription could not be started: the subscription does not open.
#[derive(Debug, Error)]
#[error("subscription `{subscription}` could not start its dedicated thread {index}")]
pub(crate) struct StartThreadError {
    subscription: String,
    index: usize,
    #[source]
    source: io::Error,
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

/// Who waits on whom: a thread on its empty ring, the loop on room.
struct Signals {
    parked: Box<[Padded<AtomicBool>]>,
    wakers: Box<[Padded<AtomicWaker>]>,
    /// 0 while the loop reads; the waited ring's index plus one, or [`ANY`], while it waits.
    loop_waiting: Padded<AtomicUsize>,
    loop_waker: AtomicWaker,
}

/// Which ring the loop waits on for room.
#[derive(Clone, Copy)]
enum Which {
    /// This one: the lane a key hashes to.
    One(usize),
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
            parked: (0..count).map(|_| Padded(AtomicBool::new(false))).collect(),
            wakers: (0..count).map(|_| Padded(AtomicWaker::new())).collect(),
            loop_waiting: Padded(AtomicUsize::new(0)),
            loop_waker: AtomicWaker::new(),
        }
    }

    /// Thread `index` freed a slot of its ring, which now holds `held` deliveries: wakes the loop
    /// if it waits on this ring, or on any ring and this one drained to half.
    fn freed(&self, index: usize, held: usize) {
        // Orders the ring's index store before the flag's load, against the loop's flag store
        // before its ring load.
        fence(Ordering::SeqCst);
        let waiting = self.loop_waiting.load(Ordering::Relaxed);
        let wakes = match waiting {
            0 => false,
            ANY => held <= RING / 2,
            one => one == index + 1,
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
            Which::One(index) => usable(index),
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

/// One thread's work: drains its ring through `handle`, parks when it is empty, and ends once the
/// loop has let go of the ring and it is empty.
// The future is built on its thread and polled there alone, so it need not be `Send`: the
// handler behind it only is.
#[allow(clippy::future_not_send)]
async fn work<Item, Handle>(
    signals: Arc<Signals>,
    index: usize,
    ring: Consumer<Item>,
    mut handle: Handle,
) where
    Handle: AsyncFnMut(Item),
{
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

/// One started thread. Dropping it cancels the thread's work where it stands, the way aborting a
/// task would; joining it waits until the thread has let go of its runtime.
struct Member {
    done: oneshot::Receiver<()>,
    abort: Option<oneshot::Sender<()>>,
}

impl Member {
    async fn join(&mut self) {
        // The abort side stays held until the thread reports, so joining cancels nothing.
        if (&mut self.done).await.is_err() {
            error!(target: "ruststream::dispatch", "a dedicated thread ended abnormally");
        }
        self.abort = None;
    }
}

/// The threads a loop started. Dropping it cancels them: a loop aborted by the shutdown timeout
/// takes its threads' work down with it.
struct Crew(Vec<Member>);

impl Crew {
    async fn join(&mut self) {
        for member in &mut self.0 {
            member.join().await;
        }
    }
}

/// Starts thread `index` of `subscription` on `runtime`, running what `work` builds there. The
/// work's future is built on the thread and never leaves it.
fn start_thread<Work, Fut>(
    subscription: &str,
    index: usize,
    runtime: Runtime,
    work: Work,
) -> Result<Member, StartThreadError>
where
    Work: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let (done_side, done) = oneshot::channel();
    let (abort, mut aborted) = oneshot::channel::<()>();
    // A thread name is a C string: a subscription name with a NUL in it would fail the spawn.
    let name = format!("{}/{index}", subscription.replace('\0', ""));
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            runtime.block_on(async move {
                let work = work();
                tokio::select! {
                    biased;
                    _ = &mut aborted => {}
                    () = work => {}
                }
            });
            // The runtime goes before the report, so a joined thread has let go of everything it
            // spawned.
            drop(runtime);
            let _ = done_side.send(());
        })
        .map_err(|source| StartThreadError {
            subscription: subscription.to_owned(),
            index,
            source,
        })?;
    Ok(Member {
        done,
        abort: Some(abort),
    })
}

/// The rings of a `threads(n)` subscription and the threads that drain them, started.
pub(super) struct Threads<Item> {
    rings: Vec<Producer<Item>>,
    signals: Arc<Signals>,
    crew: Crew,
    keyed: bool,
}

impl<Item: Send + 'static> Threads<Item> {
    /// Starts `count` threads for `subscription`, each draining its ring through the handler
    /// `each` builds for it on the thread. Everything that can fail happens here, before the
    /// subscription's loop runs: a runtime or a thread that cannot start refuses the subscription.
    pub(super) fn start<Each, Handle>(
        subscription: &str,
        count: usize,
        keyed: bool,
        mut each: Each,
    ) -> Result<Self, StartThreadError>
    where
        Each: FnMut() -> Handle,
        Handle: AsyncFnMut(Item) + Send + 'static,
    {
        let signals = Arc::new(Signals::new(count));
        let mut rings = Vec::with_capacity(count);
        let mut crew = Crew(Vec::with_capacity(count));
        for index in 0..count {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| StartThreadError {
                    subscription: subscription.to_owned(),
                    index,
                    source,
                })?;
            let (producer, consumer) = RingBuffer::new(RING);
            let signals = Arc::clone(&signals);
            let handle = each();
            crew.0
                .push(start_thread(subscription, index, runtime, move || {
                    work(signals, index, consumer, handle)
                })?);
            rings.push(producer);
        }
        Ok(Self {
            rings,
            signals,
            crew,
            keyed,
        })
    }

    /// Reads `stream` into the rings until it ends or `shutdown` fires, then lets the threads
    /// drain what their rings hold and waits for them.
    pub(super) async fn feed<Items, Failure>(
        mut self,
        stream: Items,
        name: &str,
        shutdown: &Shutdown,
        key: fn(&Item) -> Option<&[u8]>,
    ) where
        Items: Stream<Item = Result<Item, Failure>>,
        Failure: Display,
    {
        self.read(stream, name, shutdown, key).await;
        self.let_go();
        self.crew.join().await;
    }

    /// Letting go of the rings tells each thread to drain what its ring holds and end: what was
    /// read off the stream is handled and settled unless the shutdown timeout aborts the loop.
    fn let_go(&mut self) {
        self.rings.clear();
        self.signals.close();
    }

    /// Reads `stream` into the rings until it ends, `shutdown` fires, or a thread is gone.
    async fn read<Items, Failure>(
        &mut self,
        stream: Items,
        name: &str,
        shutdown: &Shutdown,
        key: fn(&Item) -> Option<&[u8]>,
    ) where
        Items: Stream<Item = Result<Item, Failure>>,
        Failure: Display,
    {
        let count = self.rings.len();
        let mut stream = pin!(stream);
        let mut cancelled = pin!(shutdown.cancelled());
        // The next ring in turn: the pool's round-robin, and where keyless deliveries of the
        // lanes go.
        let mut turn = 0usize;
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            // A pool picks its ring before it reads, so nothing comes off the stream that no ring
            // could take; a lane learns its ring from the delivery.
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
                        picked.unwrap_or_else(|| keyed.map_or(turn, |key| lane_of(key, count)));
                    if keyed.is_none() {
                        turn = (index + 1) % count;
                    }
                    if picked.is_none() {
                        // A lane waits for its own ring, whatever else has room: a key stays in
                        // order. Shutdown does not cut the wait short, so the delivery in hand is
                        // handed over rather than dropped.
                        if let Err(stuck) = self
                            .room(Which::One(index), None::<Pin<&mut Pending<()>>>)
                            .await
                        {
                            report(name, &stuck);
                            break;
                        }
                    }
                    if self.rings[index].push(item).is_err() {
                        // Only the loop fills a ring, and it pushes only after it saw room.
                        unreachable!("the loop pushes only after it saw room");
                    }
                    self.signals.pushed(index);
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
    }

    /// Waits for room by `which`, or for shutdown where `cancelled` is given.
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
    use std::sync::atomic::AtomicUsize;

    use futures::StreamExt;
    use futures::stream;
    use tokio::sync::{Notify, Semaphore};

    use super::*;

    /// The deliveries the loop may hold ahead of the handlers: one in hand per thread and a full
    /// ring each.
    const READ_AHEAD: usize = 2 * (1 + RING);

    /// With every handler held, the loop reads no more than the rings and the threads can hold;
    /// on shutdown the threads handle every delivery it read, none is dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_read_ahead_is_bounded_and_drained_on_shutdown() {
        let gate = Arc::new(Semaphore::new(0));
        let handled = Arc::new(AtomicUsize::new(0));
        let pulled = Arc::new(AtomicUsize::new(0));
        let full = Arc::new(Notify::new());
        let threads = Threads::start("bounded", 2, false, || {
            let (gate, handled) = (Arc::clone(&gate), Arc::clone(&handled));
            async move |_item: u32| {
                gate.acquire().await.expect("the gate stays open").forget();
                handled.fetch_add(1, Ordering::SeqCst);
            }
        })
        .expect("the threads start");
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
        let shutdown = Shutdown::new();
        let stopping = shutdown.clone();
        let reading = tokio::spawn(async move {
            let mut threads = threads;
            threads.read(items, "bounded", &stopping, |_| None).await;
            threads
        });
        full.notified().await;
        shutdown.cancel();
        let mut threads = reading.await.expect("the loop stops reading");
        // The rings go while every thread still holds its first delivery: what they hold must
        // be handled all the same.
        threads.let_go();
        gate.add_permits(100);
        threads.crew.join().await;
        let pulled = pulled.load(Ordering::SeqCst);
        assert!(pulled <= READ_AHEAD, "read {pulled} ahead of held handlers");
        assert_eq!(
            handled.load(Ordering::SeqCst),
            pulled,
            "a delivery read was not handled"
        );
    }

    /// A thread whose work ends early (a panic outside the handler's own catch) stops the loop
    /// rather than leaving it waiting on a ring nobody drains, the lane's strict wait included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_thread_that_dies_stops_the_loop() {
        for keyed in [false, true] {
            let threads = Threads::start("dying", 1, keyed, || {
                async move |item: u32| {
                    assert!(item > 0, "the first delivery takes the thread down");
                }
            })
            .expect("the threads start");
            let items = stream::iter(0..1000u32).map(Ok::<_, Infallible>);
            let shutdown = Shutdown::new();
            let fed = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                threads.feed(items, "dying", &shutdown, |_| Some(b"key".as_slice())),
            )
            .await;
            assert!(
                fed.is_ok(),
                "the loop waited on a dead thread (keyed: {keyed})"
            );
        }
    }
}
