//! Which tasks of the runner are runnable: the signal the test harness tells a busy app from a
//! parked one by.
//!
//! A paused clock moves only when the runtime has nothing runnable. A task that keeps itself
//! runnable stops it, so a wait on the app never ends; a task parked on a timer does not, and the
//! clock then lands exactly on the timer's deadline. The harness has to tell the two apart, and
//! tokio exposes no stable signal for it, so the runner follows its own tasks. A thread that holds
//! a [`Quiescence`] has every task the runner spawns wrapped in a future whose waker marks the
//! task runnable: a task starts runnable, becomes runnable when woken, and stops being runnable
//! when it is polled or dropped.
//!
//! Cost: nothing in a build without the `testing` feature. With it, a task spawned on a thread that
//! holds no lease pays one thread-local read at spawn; a followed one pays an atomic per wake and
//! poll.

use tokio::task::JoinHandle;

/// Spawns `work` on the runtime, followed where the thread holds a lease.
pub(crate) fn spawn_followed<Work>(work: Work) -> JoinHandle<Work::Output>
where
    Work: Future + Send + 'static,
    Work::Output: Send + 'static,
{
    tokio::spawn(follow(work))
}

/// `work` as it runs on the runner: followed where the thread holds a lease, and as it is
/// otherwise.
#[cfg(not(feature = "testing"))]
pub(crate) const fn follow<Work: Future>(work: Work) -> Work {
    work
}

#[cfg(feature = "testing")]
pub use testing::Quiescence;
#[cfg(feature = "testing")]
pub(crate) use testing::follow;

#[cfg(feature = "testing")]
mod testing {
    use std::cell::RefCell;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Context, Poll, Wake, Waker};

    use pin_project_lite::pin_project;
    use tokio::sync::Notify;
    use tokio::sync::futures::Notified;

    thread_local! {
        /// The tracking this thread's lease holds, if any.
        static LEASE: RefCell<Option<Arc<Tracking>>> = const { RefCell::new(None) };
    }

    /// The task waits for a wake.
    const PARKED: u8 = 0;
    /// The task was woken, or just spawned, and has not been polled since.
    const RUNNABLE: u8 = 1;
    /// The task is gone: a wake that arrives late changes nothing.
    const GONE: u8 = 2;

    /// What the followed tasks of a thread share.
    #[derive(Debug, Default)]
    struct Tracking {
        runnable: AtomicUsize,
        /// Signalled when a task becomes runnable.
        woken: Notify,
    }

    /// The lease on a thread's task tracking, held by a harness on the thread it runs on. Leases
    /// of one thread share one tracking, so a second harness on the thread follows with the first.
    #[doc(hidden)]
    #[derive(Debug)]
    pub struct Quiescence {
        tracking: Arc<Tracking>,
    }

    impl Quiescence {
        /// Takes the tracking of this thread.
        #[must_use]
        pub fn take() -> Self {
            let tracking = LEASE.with(|slot| {
                Arc::clone(
                    slot.borrow_mut()
                        .get_or_insert_with(|| Arc::new(Tracking::default())),
                )
            });
            Self { tracking }
        }

        /// How many followed tasks are woken and not yet polled.
        #[must_use]
        pub fn runnable(&self) -> usize {
            self.tracking.runnable.load(Ordering::Acquire)
        }

        /// Resolves once a followed task becomes runnable. A wake that came since the last wait
        /// resolves it at once; `enable` it before reading [`runnable`](Self::runnable), so a
        /// wake in between is not lost.
        pub fn woken(&self) -> Notified<'_> {
            self.tracking.woken.notified()
        }
    }

    impl Drop for Quiescence {
        fn drop(&mut self) {
            // Two holders left: this one and the thread's slot.
            if Arc::strong_count(&self.tracking) == 2 {
                let _ = LEASE.try_with(|slot| slot.borrow_mut().take());
            }
        }
    }

    /// What a followed task shares with the wakers it hands out.
    struct Flag {
        state: AtomicU8,
        tracking: Arc<Tracking>,
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
                self.tracking.runnable.fetch_add(1, Ordering::AcqRel);
                self.tracking.woken.notify_one();
            }
        }

        fn polled(&self) {
            if self
                .state
                .compare_exchange(RUNNABLE, PARKED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.tracking.runnable.fetch_sub(1, Ordering::AcqRel);
            }
        }

        fn gone(&self) {
            if self.state.swap(GONE, Ordering::AcqRel) == RUNNABLE {
                self.tracking.runnable.fetch_sub(1, Ordering::AcqRel);
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
        /// A task of the runner on a thread that holds a lease, or one that passes through.
        pub(crate) struct Followed<Work> {
            #[pin]
            work: Work,
            follow: Option<(Arc<Flag>, Waker)>,
        }

        impl<Work> PinnedDrop for Followed<Work> {
            fn drop(this: Pin<&mut Self>) {
                if let Some((flag, _)) = &this.follow {
                    flag.gone();
                }
            }
        }
    }

    impl<Work: Future> Future for Followed<Work> {
        type Output = Work::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            let Some((flag, waker)) = this.follow else {
                return this.work.poll(cx);
            };
            {
                let mut outer = flag.outer.lock().unwrap_or_else(PoisonError::into_inner);
                if !outer
                    .as_ref()
                    .is_some_and(|known| known.will_wake(cx.waker()))
                {
                    *outer = Some(cx.waker().clone());
                }
            }
            // Cleared before the poll, so a wake the poll itself causes marks the task again.
            flag.polled();
            let mut followed = Context::from_waker(waker);
            this.work.poll(&mut followed)
        }
    }

    /// Wraps `work` in a followed future where this thread holds a lease; passes it through
    /// otherwise.
    pub(crate) fn follow<Work: Future>(work: Work) -> Followed<Work> {
        let tracking = LEASE.try_with(|slot| slot.borrow().clone()).ok().flatten();
        let follow = tracking.map(|tracking| {
            tracking.runnable.fetch_add(1, Ordering::AcqRel);
            tracking.woken.notify_one();
            let flag = Arc::new(Flag {
                state: AtomicU8::new(RUNNABLE),
                tracking,
                outer: Mutex::new(None),
            });
            let waker = Waker::from(Arc::clone(&flag));
            (flag, waker)
        });
        Followed { work, follow }
    }

    #[cfg(test)]
    mod tests {
        use tokio::sync::oneshot;
        use tokio::task;

        use super::Quiescence;
        use crate::follow::spawn_followed;

        #[tokio::test]
        async fn a_spawned_task_is_runnable_until_it_is_polled() {
            let lease = Quiescence::take();
            let (_keep, parked) = oneshot::channel::<()>();
            drop(spawn_followed(async move {
                let _ = parked.await;
            }));
            assert_eq!(lease.runnable(), 1, "a task starts runnable");
            task::yield_now().await;
            assert_eq!(lease.runnable(), 0, "it ran and parked");
        }

        #[tokio::test]
        async fn a_woken_task_is_runnable_until_it_is_polled_again() {
            let lease = Quiescence::take();
            let (wake, parked) = oneshot::channel::<()>();
            drop(spawn_followed(async move {
                let _ = parked.await;
            }));
            task::yield_now().await;
            assert_eq!(lease.runnable(), 0);
            wake.send(()).expect("the task waits");
            assert_eq!(lease.runnable(), 1, "the wake marks it");
            task::yield_now().await;
            assert_eq!(lease.runnable(), 0, "it ran to its end");
        }

        #[tokio::test]
        async fn a_task_that_yields_in_a_loop_stays_runnable() {
            let lease = Quiescence::take();
            let busy = spawn_followed(async {
                loop {
                    task::yield_now().await;
                }
            });
            for _ in 0..10 {
                task::yield_now().await;
                assert_eq!(lease.runnable(), 1, "it re-wakes itself every turn");
            }
            busy.abort();
        }

        #[tokio::test]
        async fn a_dropped_runnable_task_is_not_counted() {
            let lease = Quiescence::take();
            let handle = spawn_followed(async {
                loop {
                    task::yield_now().await;
                }
            });
            handle.abort();
            let _ = handle.await;
            assert_eq!(lease.runnable(), 0);
        }

        #[tokio::test]
        async fn a_task_spawned_without_a_lease_is_not_counted() {
            let (_keep, parked) = oneshot::channel::<()>();
            let handle = spawn_followed(async move {
                let _ = parked.await;
            });
            let lease = Quiescence::take();
            assert_eq!(lease.runnable(), 0);
            handle.abort();
        }
    }
}
