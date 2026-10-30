//! Where the work a delivery leaves behind runs on tokio: a task of the runtime the delivery ran
//! on, tracked so a graceful shutdown waits for it.

use std::fmt;
use std::time::Duration;

use tokio_util::task::TaskTracker;

use crate::{Clock, Delivery, Detach, Tokio};

/// The runner of a subscription on the app's runtime: what its deliveries leave behind is a task
/// of the app's tracker, which the teardown drains.
///
/// A clone shares the tracker: it costs a reference count, never an allocation.
#[derive(Clone)]
pub(crate) struct Tracked {
    tasks: TaskTracker,
}

impl Tracked {
    /// The runner over the app's tracker `tasks`.
    pub(crate) const fn new(tasks: TaskTracker) -> Self {
        Self { tasks }
    }

    /// The app's tracker, which a dedicated thread's runner shares.
    pub(crate) const fn tasks(&self) -> &TaskTracker {
        &self.tasks
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
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        drop(self.tasks.spawn(work));
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        drop(self.tasks.spawn(async move {
            Tokio.sleep(delay).await;
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

impl Detach for OnThread {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        drop(self.tasks.spawn(self.own.track_future(work)));
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        drop(self.tasks.spawn(self.own.track_future(async move {
            Tokio.sleep(delay).await;
            work.await;
        })));
    }
}

/// What one subscription's loop dispatches under: the shared body's delivery context and the
/// runner it hands leftovers to, kept in the one allocation a subscription makes for its context.
pub(crate) struct Hosted<C> {
    pub(crate) delivery: Delivery<C>,
    pub(crate) host: Tracked,
}

impl<C> Hosted<C> {
    /// `delivery` dispatched on the app's runtime, its leftovers tracked by `tasks`.
    pub(crate) const fn new(delivery: Delivery<C>, tasks: TaskTracker) -> Self {
        Self {
            delivery,
            host: Tracked::new(tasks),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::task::TaskTracker;

    use super::Tracked;

    #[test]
    fn the_runner_debug_form_reports_its_pending_continuations() {
        let host = format!("{:?}", Tracked::new(TaskTracker::new()));
        assert!(host.contains("pending_continuations: 0"), "{host}");
    }
}
