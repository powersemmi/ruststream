//! Where the work a delivery leaves behind runs on compio: a task of the core's runtime, counted so
//! a graceful shutdown waits for it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use compio::runtime;
use event_listener::Event;
use ruststream_runtime::{Continuation, Detach};

use crate::time;

/// The work a delivery leaves behind, run on the core that detaches it and counted until it ends.
///
/// An `and_after` continuation, a post-settle hook and the timer of a deferred retry copy are tasks
/// of the runtime of that core.
///
/// A clone shares the count and costs one reference count; every core holds one. The work is
/// spawned on the core that calls, never moved: compio's tasks belong to their runtime.
///
/// Cost: one task allocation in compio's slab and one reference count per detach; a delayed one
/// adds a timer (one `BTreeMap` node). Ending is one `fetch_sub`, and the wake of a waiting
/// [`drained`](Self::drained) only when the count reaches zero.
///
/// compio's `JoinHandle` cancels its task when dropped, so every spawn here is
/// [`detach`](compio::runtime::JoinHandle::detach)ed explicitly.
///
/// # Examples
///
/// ```
/// use compio::runtime::Runtime;
/// use ruststream_runtime::Detach;
/// use ruststream_runtime_compio::Tasks;
///
/// # fn main() -> std::io::Result<()> {
/// Runtime::new()?.block_on(async {
///     let tasks = Tasks::default();
///     tasks.detach(async { /* the continuation of a delivery */ });
///     // The shutdown waits for what deliveries left behind.
///     tasks.drained().await;
///     assert_eq!(tasks.pending(), 0);
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct Tasks(Arc<Count>);

#[derive(Debug, Default)]
struct Count {
    live: AtomicUsize,
    ended: Event,
}

/// Takes one task off the count when it ends, panicked or cancelled included.
struct Live(Arc<Count>);

impl Drop for Live {
    fn drop(&mut self) {
        if self.0.live.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.ended.notify(usize::MAX);
        }
    }
}

impl Tasks {
    /// How many tasks are running or waiting on a timer.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.0.live.load(Ordering::Acquire)
    }

    /// Resolves once no task is running: the shutdown's wait for the work deliveries left behind.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping it loses nothing.
    pub async fn drained(&self) {
        while self.pending() != 0 {
            // Registered first and the count read again after, so an end in between is not lost.
            let ended = self.0.ended.listen();
            if self.pending() == 0 {
                return;
            }
            ended.await;
        }
    }

    /// Spawns `work` on this core's runtime, counted until it ends.
    fn spawn<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        self.0.live.fetch_add(1, Ordering::AcqRel);
        let live = Live(Arc::clone(&self.0));
        runtime::spawn(async move {
            let _live = live;
            work.await;
        })
        .detach();
    }
}

impl Detach for Tasks {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.spawn(work);
    }

    fn detach_boxed(&self, work: Continuation) {
        self.spawn(work);
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        // The deadline is fixed by the call, not by the first poll of the task.
        let timer = time::sleep(delay);
        self.spawn(async move {
            timer.await;
            work.await;
        });
    }

    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + Send + 'static,
        Work: Future<Output = ()> + Send + 'static,
    {
        // The work is built once the timer has fired, so the task holds the larger of the two
        // rather than both.
        let timer = time::sleep(delay);
        self.spawn(async move {
            timer.await;
            make().await;
        });
    }
}
