//! Who waits on whom in the ring hand-off: a thread parks on its empty ring until the loop pushes
//! to it, and the loop waits for room until a ring has drained to half or, stopping, until its
//! key's ring frees a slot.

use std::boxed::Box;
use std::iter::repeat_with;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::task::{Context as TaskContext, Poll};

use futures::task::AtomicWaker;
use rtrb::Producer;

use super::RING;

/// A value on a cache line of its own, so what one side writes does not evict what the other
/// reads.
#[repr(align(128))]
pub(super) struct Padded<T>(T);

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
pub(super) const STOPPING: usize = 1 << (usize::BITS - 1);

/// Who waits on whom: a thread on its empty ring, the loop on room.
pub(super) struct Signals {
    pub(super) parked: Box<[Padded<AtomicBool>]>,
    pub(super) wakers: Box<[Padded<AtomicWaker>]>,
    /// 0 while the loop reads; while it waits, [`ANY`] or the waited ring's index plus one, with
    /// [`STOPPING`] set where the ring's first free slot will do.
    pub(super) loop_waiting: Padded<AtomicUsize>,
    loop_waker: AtomicWaker,
}

/// Which ring the loop waits on for room.
#[derive(Clone, Copy)]
pub(super) enum Which {
    /// This one, the thread a key hashes to, until it has drained to half.
    One(usize),
    /// This one, at its first free slot: shutdown came, and the delivery in hand goes before the
    /// loop stops.
    Stopping(usize),
    /// The first with room from here on, in turn.
    Next(usize),
}

impl Signals {
    pub(super) fn new(count: usize) -> Self {
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
    pub(super) fn freed(&self, index: usize, held: usize) {
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
    pub(super) fn pushed(&self, index: usize) {
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
    pub(super) fn poll_room<Item>(
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
    pub(super) fn close(&self) {
        for waker in &*self.wakers {
            waker.wake();
        }
    }
}

/// Wakes the loop when a thread's work ends, however it ends: a ring the loop waits on is then
/// found abandoned rather than waited on forever.
pub(super) struct Leaving<'a>(pub(super) &'a Signals);

impl Drop for Leaving<'_> {
    fn drop(&mut self) {
        self.0.leaving();
    }
}
