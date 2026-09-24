//! Where a subscription's workers run, decided once, when the loop starts.

use std::future::Future;
use std::thread;

use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tracing::error;

/// Where a subscription's workers run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Placement {
    /// As tasks of the runtime the loop runs on.
    Runtime,
    /// Each worker on a thread of its own running a current-thread runtime, so a worker's handler
    /// computes there without holding a thread of the app's runtime, and its timers and the tasks
    /// it spawns run there too.
    Pinned,
}

/// One started worker, wherever it runs.
pub(super) enum Member {
    /// A task of the loop's runtime.
    Task(JoinHandle<()>),
    /// A thread of its own. Dropping `abort` cancels the worker's future where it stands, the
    /// way aborting a task would; `done` resolves once the thread has let go of its runtime.
    Thread {
        done: oneshot::Receiver<()>,
        abort: oneshot::Sender<()>,
    },
}

impl Member {
    /// Waits for the worker to finish, logging a failure.
    pub(super) async fn join(self) {
        match self {
            Self::Task(handle) => {
                if let Err(err) = handle.await {
                    log_failure(&err);
                }
            }
            Self::Thread { done, abort } => {
                // Held until the thread reports back, so the worker is not cancelled by the join.
                if done.await.is_err() {
                    error!(target: "ruststream::dispatch", "worker thread failed");
                }
                drop(abort);
            }
        }
    }

    /// Cancels the worker where it stands.
    pub(super) fn abort(&mut self) {
        match self {
            Self::Task(handle) => handle.abort(),
            Self::Thread { abort, .. } => {
                // Replacing the sender drops the one the worker listens on.
                *abort = oneshot::channel().0;
            }
        }
    }
}

fn log_failure(err: &JoinError) {
    error!(target: "ruststream::dispatch", error = %err, "worker task failed");
}

impl Placement {
    /// Starts worker `index`.
    pub(super) fn start<Work, Fut>(self, index: usize, work: Work) -> Member
    where
        Work: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match self {
            Self::Runtime => Member::Task(tokio::spawn(work())),
            Self::Pinned => {
                let (done_side, done) = oneshot::channel();
                let (abort, mut aborted) = oneshot::channel::<()>();
                let spawned = thread::Builder::new()
                    .name(format!("ruststream-worker-{index}"))
                    .spawn(move || {
                        let runtime = match Builder::new_current_thread().enable_all().build() {
                            Ok(runtime) => runtime,
                            Err(err) => {
                                error!(
                                    target: "ruststream::dispatch",
                                    error = %err,
                                    "a worker thread could not build its runtime",
                                );
                                return;
                            }
                        };
                        runtime.block_on(async move {
                            let work = work();
                            tokio::select! {
                                biased;
                                _ = &mut aborted => {}
                                () = work => {}
                            }
                        });
                        // The runtime goes before the report, so a joined worker has let go of
                        // everything it spawned on its thread.
                        drop(runtime);
                        let _ = done_side.send(());
                    });
                if let Err(err) = spawned {
                    error!(
                        target: "ruststream::dispatch",
                        error = %err,
                        "a worker thread could not start",
                    );
                }
                Member::Thread { done, abort }
            }
        }
    }
}
