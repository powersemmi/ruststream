//! Where the work a delivery leaves behind runs on tokio: a task of the runtime the delivery ran
//! on, tracked so a graceful shutdown waits for it.

use std::fmt;
use std::time::Duration;

use tokio::time;
use tokio_util::task::TaskTracker;

use crate::BoxFuture;
use crate::Detach;

/// The runner of a subscription on the app's runtime: what its deliveries leave behind is a task
/// of the app's tracker, which the teardown drains.
pub(crate) struct Tracked {
    tasks: TaskTracker,
}

impl Tracked {
    /// The runner over the app's tracker `tasks`.
    pub(crate) const fn new(tasks: TaskTracker) -> Self {
        Self { tasks }
    }
}

impl fmt::Debug for Tracked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracked")
            .field("pending_continuations", &self.tasks.len())
            .finish()
    }
}

impl Detach for Tracked {
    fn detach(&self, work: BoxFuture<'static, ()>) {
        drop(self.tasks.spawn(work));
    }

    fn detach_after(&self, delay: Duration, work: BoxFuture<'static, ()>) {
        drop(self.tasks.spawn(async move {
            time::sleep(delay).await;
            work.await;
        }));
    }
}

/// The runner of a dedicated thread (`threads(n)`): what its deliveries leave behind runs on the
/// thread's runtime, tracked by the app, which the teardown drains, and by the thread, which
/// waits for it before it ends so a shutdown does not drop it with the thread's runtime.
pub(crate) struct OnThread {
    tasks: TaskTracker,
    own: TaskTracker,
}

impl OnThread {
    /// The runner of one thread: the app's tracker `tasks` and the thread's own, `own`.
    pub(crate) const fn new(tasks: TaskTracker, own: TaskTracker) -> Self {
        Self { tasks, own }
    }
}

impl fmt::Debug for OnThread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnThread")
            .field("pending_continuations", &self.tasks.len())
            .field("pending_on_thread", &self.own.len())
            .finish()
    }
}

impl Detach for OnThread {
    fn detach(&self, work: BoxFuture<'static, ()>) {
        drop(self.tasks.spawn(self.own.track_future(work)));
    }

    fn detach_after(&self, delay: Duration, work: BoxFuture<'static, ()>) {
        drop(self.tasks.spawn(self.own.track_future(async move {
            time::sleep(delay).await;
            work.await;
        })));
    }
}
