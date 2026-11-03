//! Which tasks of the core's runtime are runnable: the signal the test harness settles on.
//!
//! compio exposes no signal that its runtime has run out of work, and `Runtime::run` re-enters the
//! executor's queue, which a task must not do. The runner follows its own tasks instead. A thread
//! that holds a [`Quiescence`] has every task [`spawn`](crate::spawn::spawn) starts wrapped in a
//! future whose waker marks the task runnable: a task starts runnable, becomes runnable when
//! woken, and stops being runnable when it is polled or dropped. The harness is settled when no
//! task is runnable after it has yielded, so every task woken before the yield has run.
//!
//! Cost: nothing in a build without the `testing` feature. With it, a task on a thread that holds
//! no lease pays one thread-local read at spawn; a followed one pays an atomic per wake and poll.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

use pin_project_lite::pin_project;

thread_local! {
    /// The count of the lease this thread holds, if any.
    static LEASE: RefCell<Option<Arc<AtomicUsize>>> = const { RefCell::new(None) };
}

/// The task waits for a wake.
const PARKED: u8 = 0;
/// The task was woken, or just spawned, and has not been polled since.
const RUNNABLE: u8 = 1;
/// The task is gone: a wake that arrives late changes nothing.
const GONE: u8 = 2;

/// The lease on a thread's task tracking, held by one harness on the thread it runs on.
///
/// # Panics
///
/// [`take`](Self::take) panics where the thread already holds a lease.
#[doc(hidden)]
#[derive(Debug)]
pub struct Quiescence {
    runnable: Arc<AtomicUsize>,
    // Not `Send`: the tracking is the thread's.
    _thread: PhantomData<Rc<()>>,
}

impl Quiescence {
    /// Takes the tracking of this thread.
    ///
    /// # Panics
    ///
    /// Panics if this thread already holds a lease.
    #[must_use]
    pub fn take() -> Self {
        let runnable = Arc::new(AtomicUsize::new(0));
        let taken = LEASE.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_some() {
                return false;
            }
            *slot = Some(Arc::clone(&runnable));
            true
        });
        assert!(
            taken,
            "task tracking is already held on this thread: one TestApp runs at a time"
        );
        Self {
            runnable,
            _thread: PhantomData,
        }
    }

    /// How many followed tasks are woken and not yet polled.
    #[must_use]
    pub fn runnable(&self) -> usize {
        self.runnable.load(Ordering::Acquire)
    }

    /// Follows `work` the way a spawned task is followed, for a future the harness polls itself:
    /// its wakes count as runnable until it is polled again.
    pub fn follow<Work: Future>(&self, work: Work) -> impl Future<Output = Work::Output> {
        followed(Arc::clone(&self.runnable), work)
    }

    /// A reader of the count that can go where the lease cannot: into a publish sink, which is
    /// `Send`.
    #[must_use]
    pub fn reader(&self) -> Runnable {
        Runnable(Arc::clone(&self.runnable))
    }
}

/// The count of runnable tasks of a lease, readable from anywhere.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Runnable(Arc<AtomicUsize>);

impl Runnable {
    /// How many followed tasks are woken and not yet polled.
    #[must_use]
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

impl Drop for Quiescence {
    fn drop(&mut self) {
        LEASE.with(|slot| slot.borrow_mut().take());
    }
}

/// What a followed task shares with the wakers it hands out.
struct Flag {
    state: AtomicU8,
    runnable: Arc<AtomicUsize>,
    /// The waker of the runtime, which a wake is passed on to.
    outer: Mutex<Option<Waker>>,
}

impl Flag {
    fn mark(&self) {
        if self
            .state
            .compare_exchange(PARKED, RUNNABLE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.runnable.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn polled(&self) {
        if self
            .state
            .compare_exchange(RUNNABLE, PARKED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.runnable.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn gone(&self) {
        if self.state.swap(GONE, Ordering::AcqRel) == RUNNABLE {
            self.runnable.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark();
        let outer = self
            .outer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(outer) = outer {
            outer.wake();
        }
    }
}

pin_project! {
    /// A task of the runner on a thread that holds a lease.
    pub(crate) struct Followed<Work> {
        #[pin]
        work: Work,
        flag: Arc<Flag>,
        waker: Waker,
    }

    impl<Work> PinnedDrop for Followed<Work> {
        fn drop(this: Pin<&mut Self>) {
            this.flag.gone();
        }
    }
}

impl<Work: Future> Future for Followed<Work> {
    type Output = Work::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        {
            let mut outer = this
                .flag
                .outer
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !outer
                .as_ref()
                .is_some_and(|known| known.will_wake(cx.waker()))
            {
                *outer = Some(cx.waker().clone());
            }
        }
        // Cleared before the poll, so a wake the poll itself causes marks the task again.
        this.flag.polled();
        let mut followed = Context::from_waker(this.waker);
        this.work.poll(&mut followed)
    }
}

/// Wraps `work` in a followed future where this thread holds a lease; gives it back otherwise.
pub(crate) fn follow<Work: Future>(work: Work) -> Result<Followed<Work>, Work> {
    let Some(runnable) = LEASE.try_with(|slot| slot.borrow().clone()).ok().flatten() else {
        return Err(work);
    };
    Ok(followed(runnable, work))
}

fn followed<Work: Future>(runnable: Arc<AtomicUsize>, work: Work) -> Followed<Work> {
    runnable.fetch_add(1, Ordering::AcqRel);
    let flag = Arc::new(Flag {
        state: AtomicU8::new(RUNNABLE),
        runnable,
        outer: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&flag));
    Followed { work, flag, waker }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Waker};

    use compio::runtime::Runtime;
    use futures::channel::oneshot;

    use super::Quiescence;
    use crate::spawn::spawn;
    use crate::yield_now;

    #[test]
    fn a_spawned_task_is_runnable_until_it_is_polled() {
        Runtime::new().expect("a runtime").block_on(async {
            let lease = Quiescence::take();
            let (_keep, parked) = oneshot::channel::<()>();
            spawn(async move {
                let _ = parked.await;
            })
            .detach();
            assert_eq!(lease.runnable(), 1, "a task starts runnable");
            yield_now().await;
            assert_eq!(lease.runnable(), 0, "it ran and parked");
        });
    }

    #[test]
    fn a_woken_task_is_runnable_until_it_is_polled_again() {
        Runtime::new().expect("a runtime").block_on(async {
            let lease = Quiescence::take();
            let (wake, parked) = oneshot::channel::<()>();
            spawn(async move {
                let _ = parked.await;
            })
            .detach();
            yield_now().await;
            assert_eq!(lease.runnable(), 0);
            wake.send(()).expect("the task waits");
            assert_eq!(lease.runnable(), 1, "the wake marks it");
            yield_now().await;
            assert_eq!(lease.runnable(), 0, "it ran to its end");
        });
    }

    #[test]
    fn each_woken_task_counts_once() {
        Runtime::new().expect("a runtime").block_on(async {
            let lease = Quiescence::take();
            let (first, parked) = oneshot::channel::<()>();
            spawn(async move {
                let _ = parked.await;
            })
            .detach();
            let (second, also) = oneshot::channel::<()>();
            spawn(async move {
                let _ = also.await;
            })
            .detach();
            yield_now().await;
            first.send(()).expect("waits");
            second.send(()).expect("waits");
            assert_eq!(lease.runnable(), 2, "one per task");
            yield_now().await;
            assert_eq!(lease.runnable(), 0);
        });
    }

    #[test]
    fn a_dropped_task_is_not_runnable() {
        Runtime::new().expect("a runtime").block_on(async {
            let lease = Quiescence::take();
            let (_keep, parked) = oneshot::channel::<()>();
            let task = spawn(async move {
                let _ = parked.await;
            });
            assert_eq!(lease.runnable(), 1);
            drop(task);
            // compio drops a cancelled task's future on its next turn.
            yield_now().await;
            assert_eq!(lease.runnable(), 0, "cancelled before its first poll");
        });
    }

    #[test]
    fn a_future_the_harness_polls_is_runnable_after_it_wakes_itself() {
        Runtime::new().expect("a runtime").block_on(async {
            let lease = Quiescence::take();
            let mut work = pin!(lease.follow(yield_now()));
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            assert_eq!(lease.runnable(), 1, "followed work starts runnable");
            assert!(work.as_mut().poll(&mut cx).is_pending());
            assert_eq!(lease.runnable(), 1, "the yield woke it while it was polled");
            assert!(work.as_mut().poll(&mut cx).is_ready());
            assert_eq!(lease.runnable(), 0, "it ran to its end");
        });
    }

    #[test]
    fn a_task_spawned_without_a_lease_is_not_followed() {
        Runtime::new().expect("a runtime").block_on(async {
            let (_keep, parked) = oneshot::channel::<()>();
            spawn(async move {
                let _ = parked.await;
            })
            .detach();
            let lease = Quiescence::take();
            assert_eq!(lease.runnable(), 0);
        });
    }

    #[test]
    fn a_task_spawned_after_the_lease_ended_is_not_followed() {
        Runtime::new().expect("a runtime").block_on(async {
            drop(Quiescence::take());
            let (_keep, parked) = oneshot::channel::<()>();
            spawn(async move {
                let _ = parked.await;
            })
            .detach();
            assert_eq!(Quiescence::take().runnable(), 0);
        });
    }

    #[test]
    fn a_reader_sees_the_count_of_its_lease() {
        Runtime::new().expect("a runtime").block_on(async {
            let lease = Quiescence::take();
            let reader = lease.reader();
            let (_keep, parked) = oneshot::channel::<()>();
            spawn(async move {
                let _ = parked.await;
            })
            .detach();
            assert_eq!(reader.count(), 1);
            yield_now().await;
            assert_eq!(reader.count(), 0);
        });
    }

    #[test]
    #[should_panic(expected = "already held")]
    fn a_second_lease_on_a_thread_panics() {
        let _first = Quiescence::take();
        let _second = Quiescence::take();
    }
}
