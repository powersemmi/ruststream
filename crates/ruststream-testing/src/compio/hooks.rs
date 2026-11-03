//! The test hooks the harness runs beside the app, kept so the shutdown can end them.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::channel::oneshot;
use futures::future::{AbortHandle, Abortable, FutureExt, Shared, join_all};
use ruststream_runtime::testing::Coordinator;
use ruststream_runtime_compio::testing::spawn_detached;

/// A hook task: the way to stop it, and the signal that it is gone.
struct Running {
    abort: AbortHandle,
    gone: Shared<oneshot::Receiver<()>>,
}

/// The tasks the harness started for the hooks a dispatch or the test's own publish handed over.
///
/// Cloning shares the set, so the handle a test publishes through starts hooks into the same set
/// the shutdown ends.
#[derive(Clone)]
pub(super) struct HookTasks {
    coordinator: Coordinator,
    running: Arc<Mutex<Vec<Running>>>,
}

impl fmt::Debug for HookTasks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookTasks")
            .field("running", &self.lock().len())
            .finish_non_exhaustive()
    }
}

impl HookTasks {
    pub(super) fn new(coordinator: Coordinator) -> Self {
        Self {
            coordinator,
            running: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // Why a lock: under the `tokio` feature a publish sink is `Send`, and it holds this set. The
    // harness runs on one thread, so the lock is never contended.
    fn lock(&self) -> MutexGuard<'_, Vec<Running>> {
        self.running.lock().expect("the hook set is poisoned")
    }

    /// Starts, as tasks the harness follows, the hooks the dispatches handed over: a dispatch waits
    /// for its hooks, so they run on the app's runtime beside it.
    pub(super) fn start(&self) {
        let mut running = self.lock();
        // A hook that ended has sent its signal; the set keeps only the ones still going.
        running.retain_mut(|hook| hook.gone.peek().is_none());
        for pending in self.coordinator.hooks().take_pending() {
            let (abort, registration) = AbortHandle::new_pair();
            let (done, gone) = oneshot::channel();
            let gone = gone.shared();
            let hook = Abortable::new(pending.into_future(self.coordinator.clone()), registration);
            spawn_detached(async move {
                let _ = hook.await;
                let _ = done.send(());
            });
            running.push(Running { abort, gone });
        }
    }

    /// Resolves once every hook started so far has ended.
    pub(super) async fn ended(&self) {
        self.start();
        let gone: Vec<_> = self.lock().iter().map(|hook| hook.gone.clone()).collect();
        join_all(gone).await;
    }

    /// Stops the hooks still going and resolves once each is dropped, so nothing of a hook runs
    /// after the harness is shut down.
    pub(super) async fn cancel(&self) {
        self.start();
        for hook in self.lock().iter() {
            hook.abort.abort();
        }
        self.ended().await;
        self.lock().clear();
    }
}
