//! The timers of one core on the wall clock: a heap of deadlines the core's driver task fires.
//!
//! compio's own timers belong to the runtime that made them and cannot leave its thread, so a
//! [`Sleep`](super::Sleep) built on them would not be `Send`. The wheel is the part of a timer that
//! can: a sleep holds a reference to the wheel of the core that first polled it and a key into
//! it, and the driver task of that core wakes the sleeps that fell due. Nothing in a sleep is tied
//! to a thread.
//!
//! The deadlines sit in a binary heap and the wakers in a slab of reusable slots. A cancelled
//! sleep frees its slot at once and leaves its heap entry to be skipped when it surfaces, or swept
//! when such entries outnumber the live ones.
//!
//! Cost: arming is one lock, one heap push and one slot, and cancelling one lock and one slot; each
//! poll after the first is one lock. Once the heap and the slab have grown to the number of sleeps
//! a core keeps, none of it allocates. The wheel and its driver task are made once per core, on
//! its first sleep. The driver waits on one compio timer at a time, which costs what compio's
//! own timer costs (two `BTreeMap` node allocations per timer that is armed alone and fires).

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem;
use std::pin::{Pin, pin};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use crate::spawn::spawn;
use compio::time::sleep_until;
use futures::{FutureExt, select_biased};

thread_local! {
    /// The wheel of the runtime this thread runs, once a sleep needed it.
    static WHEEL: Cell<Option<&'static Wheel>> = const { Cell::new(None) };
}

/// Stale heap entries a wheel tolerates before it counts them against the live ones.
const SWEEP_FLOOR: usize = 64;

/// One armed timer: its slot, and which use of the slot it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Key {
    /// Which driver the timer was armed under: a timer of an earlier one is gone with it.
    epoch: u64,
    slot: usize,
    generation: u64,
}

/// What a poll of an armed timer found.
pub(super) enum Status {
    /// The deadline has passed.
    Ready,
    /// The timer waits for its deadline.
    Pending,
    /// The runtime that drove the wheel is gone, and the timer must be armed again.
    Orphaned,
}

/// The waker of one timer; a slot is reused by the next timer once it is free.
#[derive(Debug, Default)]
struct Slot {
    generation: u64,
    waker: Option<Waker>,
    armed: bool,
}

/// The place of a timer in the heap, ordered by its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    deadline: Instant,
    slot: usize,
    generation: u64,
}

#[derive(Debug, Default)]
struct Timers {
    slots: Vec<Slot>,
    free: Vec<usize>,
    heap: BinaryHeap<Reverse<Entry>>,
    /// Heap entries whose timer is gone.
    stale: usize,
    /// The driver task, parked until a timer earlier than the one it waits for is armed.
    driver: Option<Waker>,
    /// An earlier timer was armed since the driver last looked.
    earlier: bool,
    /// A driver task runs the wheel; it is gone when its runtime ends.
    driven: bool,
    /// Counts the drivers that ended, so a key never reaches a slot a later driver reused.
    epoch: u64,
}

impl Timers {
    fn is_armed(&self, key: &Key) -> bool {
        key.epoch == self.epoch
            && self
                .slots
                .get(key.slot)
                .is_some_and(|slot| slot.armed && slot.generation == key.generation)
    }

    fn is_live(&self, entry: &Entry) -> bool {
        self.is_armed(&Key {
            epoch: self.epoch,
            slot: entry.slot,
            generation: entry.generation,
        })
    }

    /// Drops the entries of cancelled timers once they outnumber the live ones.
    fn sweep(&mut self) {
        let live = self.slots.len() - self.free.len();
        if self.stale <= SWEEP_FLOOR.max(live) {
            return;
        }
        let mut heap = mem::take(&mut self.heap);
        heap.retain(|Reverse(entry)| self.is_live(entry));
        self.heap = heap;
        self.stale = 0;
    }
}

/// The timers of one core.
#[derive(Debug, Default)]
pub(super) struct Wheel {
    timers: Mutex<Timers>,
}

impl Wheel {
    /// The wheel of this thread, with a driver task on the runtime the thread runs now.
    ///
    /// The wheel is made on the thread's first sleep and lives as long as the process: one per
    /// thread that ever slept. A runtime that ended leaves it without a driver, and the next
    /// call gives it a driver on the new runtime.
    ///
    /// # Panics
    ///
    /// Panics when the thread runs no compio runtime, as compio's own timers do.
    pub(super) fn current() -> &'static Self {
        let wheel = WHEEL.get().unwrap_or_else(|| {
            let wheel: &'static Self = Box::leak(Box::default());
            WHEEL.set(Some(wheel));
            wheel
        });
        if wheel.take_driver_seat() {
            spawn(drive(wheel)).detach();
        }
        wheel
    }

    /// Marks the wheel driven; `false` while a driver still runs it.
    fn take_driver_seat(&self) -> bool {
        !mem::replace(&mut self.lock().driven, true)
    }

    fn lock(&self) -> MutexGuard<'_, Timers> {
        self.timers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Arms a timer for `deadline`; `None` where it has already passed.
    pub(super) fn arm(&self, deadline: Instant, waker: &Waker) -> Option<Key> {
        if deadline <= Instant::now() {
            return None;
        }
        let mut timers = self.lock();
        let slot = timers.free.pop().unwrap_or_else(|| {
            timers.slots.push(Slot::default());
            timers.slots.len() - 1
        });
        let place = &mut timers.slots[slot];
        place.generation += 1;
        place.armed = true;
        place.waker = Some(waker.clone());
        let generation = place.generation;
        let epoch = timers.epoch;
        let earliest = timers
            .heap
            .peek()
            .is_none_or(|Reverse(first)| deadline < first.deadline);
        timers.heap.push(Reverse(Entry {
            deadline,
            slot,
            generation,
        }));
        let driver = if earliest {
            timers.earlier = true;
            timers.driver.take()
        } else {
            None
        };
        drop(timers);
        // Outside the lock: the driver may be on another thread and take it at once.
        if let Some(driver) = driver {
            driver.wake();
        }
        Some(Key {
            epoch,
            slot,
            generation,
        })
    }

    /// Polls an armed timer, keeping `waker` as the one to wake.
    pub(super) fn poll(&self, key: &Key, waker: &Waker) -> Status {
        let mut timers = self.lock();
        if !timers.driven || key.epoch != timers.epoch {
            return Status::Orphaned;
        }
        if !timers.is_armed(key) {
            return Status::Ready;
        }
        let slot = &mut timers.slots[key.slot];
        if !slot
            .waker
            .as_ref()
            .is_some_and(|armed| armed.will_wake(waker))
        {
            slot.waker = Some(waker.clone());
        }
        drop(timers);
        Status::Pending
    }

    /// Frees the slot of a timer: its sleep was dropped before it fired.
    pub(super) fn cancel(&self, key: &Key) {
        let mut timers = self.lock();
        if !timers.is_armed(key) {
            return;
        }
        let slot = &mut timers.slots[key.slot];
        slot.armed = false;
        let waker = slot.waker.take();
        timers.free.push(key.slot);
        timers.stale += 1;
        timers.sweep();
        drop(timers);
        // A waker dropped outside the lock may run a destructor of the task.
        drop(waker);
    }
}

/// Marks the wheel orphaned when its driver task is dropped with the runtime.
struct Orphan(&'static Wheel);

impl Drop for Orphan {
    fn drop(&mut self) {
        let gone = {
            let mut timers = self.0.lock();
            // A fresh state: the free list and the counters pointed into the slots that go.
            let next = Timers {
                epoch: timers.epoch + 1,
                ..Timers::default()
            };
            mem::replace(&mut *timers, next)
        };
        // Outside the lock: a sleep waiting on another core polls again, finds the wheel orphaned
        // and arms itself where it runs; a waker only dropped would leave it waiting for good.
        for slot in gone.slots {
            if let Some(waker) = slot.waker {
                waker.wake();
            }
        }
    }
}

/// Resolves when a timer earlier than the one the driver waits for was armed.
struct Earlier<'wheel>(&'wheel Wheel);

impl Future for Earlier<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut timers = self.0.lock();
        if mem::take(&mut timers.earlier) {
            return Poll::Ready(());
        }
        if !timers
            .driver
            .as_ref()
            .is_some_and(|parked| parked.will_wake(cx.waker()))
        {
            timers.driver = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

/// The task of a core that wakes the sleeps of its wheel as their deadlines pass.
// Why: the task waits on a compio timer, which belongs to its core; it is spawned there and never
// moves.
#[allow(clippy::future_not_send)]
async fn drive(wheel: &'static Wheel) {
    let _orphan = Orphan(wheel);
    let mut due: Vec<Waker> = Vec::new();
    // compio's timer map frees its node when its last timer goes, so the next timer would allocate
    // one again. A timer that never fires keeps the node, and every timer after it fits in it.

    loop {
        let next = {
            let mut timers = wheel.lock();
            let now = Instant::now();
            while let Some(&Reverse(first)) = timers.heap.peek() {
                if first.deadline > now {
                    break;
                }
                timers.heap.pop();
                if timers.is_live(&first) {
                    let slot = &mut timers.slots[first.slot];
                    slot.armed = false;
                    due.extend(slot.waker.take());
                    timers.free.push(first.slot);
                } else {
                    timers.stale -= 1;
                }
            }
            timers.earlier = false;
            timers.heap.peek().map(|Reverse(first)| first.deadline)
        };
        for waker in &due {
            waker.wake_by_ref();
        }
        // Cleared, not drained: the buffer keeps its room for the next round.
        due.clear();
        let mut earlier = pin!(Earlier(wheel).fuse());
        match next {
            Some(deadline) => {
                let mut passed = pin!(sleep_until(deadline).fuse());
                select_biased! {
                    () = passed => {},
                    () = earlier => {},
                }
            }
            None => {
                select_biased! {
                    () = earlier => {},
                }
            }
        }
    }
}
