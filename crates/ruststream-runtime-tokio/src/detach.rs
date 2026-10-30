//! Where the work a delivery leaves behind runs on tokio: a task of the runtime the delivery ran
//! on, tracked so a graceful shutdown waits for it.

use std::fmt;
use std::time::Duration;

use tokio_util::task::TaskTracker;

use crate::{Clock, Delivery, Detach, Lent, Tokio};

/// The tokio runner's handle: what a delivery leaves behind is a task of the app's tracker, which
/// the teardown drains. On a dedicated thread (`threads(n)`) the task runs on the thread's runtime
/// and the thread tracks it too, waiting for it before it ends so a shutdown does not drop it
/// with the thread's runtime.
///
/// One type for every placement, so the delivery body is instantiated once per subscription
/// shape and the compiler inlines the handler into it as it does with a single caller. A clone
/// shares the trackers: it costs reference counts, never an allocation.
#[derive(Clone)]
pub(crate) struct Tracked {
    tasks: TaskTracker,
    own: Option<TaskTracker>,
}

impl Tracked {
    /// The runner over the app's tracker `tasks`.
    pub(crate) const fn new(tasks: TaskTracker) -> Self {
        Self { tasks, own: None }
    }

    /// The runner of one dedicated thread: the app's tracker and the thread's own, `own`.
    pub(crate) fn on_thread(&self, own: TaskTracker) -> Self {
        Self {
            tasks: self.tasks.clone(),
            own: Some(own),
        }
    }

    /// Spawns `work`, tracked by the app and, on a dedicated thread, by the thread.
    fn spawn<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        match &self.own {
            None => drop(self.tasks.spawn(work)),
            Some(own) => drop(self.tasks.spawn(own.track_future(work))),
        }
    }
}

impl fmt::Debug for Tracked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracked")
            .field("pending_continuations", &self.tasks.len())
            .field("on_dedicated_thread", &self.own.is_some())
            .finish()
    }
}

impl Detach for Tracked {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.spawn(work);
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.spawn(async move {
            Tokio.sleep(delay).await;
            work.await;
        });
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

impl<C> Lent<C> for Hosted<C> {
    type Host = Tracked;

    #[inline]
    fn delivery(&self) -> &Delivery<C> {
        &self.delivery
    }

    #[inline]
    fn host(&self) -> &Tracked {
        &self.host
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
