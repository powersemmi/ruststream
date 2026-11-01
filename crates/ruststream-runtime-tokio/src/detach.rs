//! Where the work a delivery leaves behind runs on tokio: a task of the runtime the delivery ran
//! on, tracked so a graceful shutdown waits for it.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::task::TaskTracker;

use crate::{Clock, Copying, Delivery, Detach, Lent, RetryCopies, RetryRoute, Tokio};

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

    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + Send + 'static,
        Work: Future<Output = ()> + Send + 'static,
    {
        // The work is built once the timer has fired, so the task holds the larger of the two
        // rather than both.
        self.spawn(async move {
            Tokio.sleep(delay).await;
            make().await;
        });
    }
}

/// What one subscription's loop dispatches under: the shared body's delivery context and the
/// runner it hands leftovers to, with the registration's retry route in its concrete type, kept
/// in the one allocation a subscription makes for its context.
pub(crate) struct Hosted<C, Retry> {
    pub(crate) delivery: Delivery<C>,
    pub(crate) host: Copying<Tracked, Retry>,
}

impl<C, Retry> Hosted<C, Retry> {
    /// `delivery` dispatched on the app's runtime, its leftovers tracked by `tasks`, its retry
    /// copies leaving through `route`.
    pub(crate) fn new(
        delivery: Delivery<C>,
        route: Option<RetryRoute<Retry>>,
        tasks: TaskTracker,
    ) -> Self {
        Self {
            delivery,
            host: Copying::new(Tracked::new(tasks), route.map(Arc::new)),
        }
    }
}

/// The runner of one dedicated thread: `host` with the thread's own tracker `own`, sharing the
/// registration's retry route.
pub(crate) fn on_thread<Retry>(
    host: &Copying<Tracked, Retry>,
    own: TaskTracker,
) -> Copying<Tracked, Retry> {
    Copying::new(host.host.on_thread(own), host.route.clone())
}

impl<C, Retry: RetryCopies<C>> Lent<C> for Hosted<C, Retry> {
    type Host = Copying<Tracked, Retry>;

    #[inline]
    fn delivery(&self) -> &Delivery<C> {
        &self.delivery
    }

    #[inline]
    fn host(&self) -> &Copying<Tracked, Retry> {
        &self.host
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::time;
    use tokio_util::task::TaskTracker;

    use super::Tracked;
    use crate::Detach;

    #[test]
    fn the_runner_debug_form_reports_its_pending_continuations() {
        let host = format!("{:?}", Tracked::new(TaskTracker::new()));
        assert!(host.contains("pending_continuations: 0"), "{host}");
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_work_is_built_once_its_delay_has_passed() {
        let tasks = TaskTracker::new();
        let host = Tracked::new(tasks.clone());
        let built = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&built);

        host.detach_after_with(Duration::from_secs(5), move || {
            flag.store(true, Ordering::SeqCst);
            async {}
        });

        time::advance(Duration::from_secs(4)).await;
        assert!(
            !built.load(Ordering::SeqCst),
            "the work is not built before its delay has passed"
        );
        tasks.close();
        tasks.wait().await;
        assert!(
            built.load(Ordering::SeqCst),
            "the tracked task built and ran the work after the delay"
        );
    }
}
