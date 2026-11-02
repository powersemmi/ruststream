//! Which core each part of a subscription runs on: the placement table made at startup, and the
//! mailbox through which core 0 starts a lane on another core.

use std::fmt::Write;
use std::sync::{Arc, Mutex};

use compio::runtime::JoinHandle;
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::future::{self, Either};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, pin_mut};
use ruststream_runtime::{Placement, ShutdownWait, Workers};
use thiserror::Error;
use tracing::error;

use crate::Stop;

/// One mount as the placement table sees it: its name and the concurrency it declared.
#[derive(Clone, Copy, Debug)]
pub struct Mount<'name> {
    /// The subscription's name, as the startup error names it.
    pub name: &'name str,
    /// The declared `workers(..)` or `threads(..)`.
    pub workers: Workers,
}

/// The `threads` mounts of a service ask for more cores than it has besides core 0.
#[derive(Debug, Error)]
#[error(
    "the threads mounts ask for {asked} dedicated cores and the service has {available} besides \
     core 0 ({mounts})"
)]
pub struct LayoutError {
    asked: usize,
    available: usize,
    mounts: String,
}

/// The cores of every mount, fixed once at startup from the mounts and the number of cores.
///
/// A `threads(n)` mount holds `n` cores of its own, taken in mount order from core 1 up; core 0 is
/// the main runtime and is never dedicated. A `workers(n)` mount is placed over the shared cores,
/// every core no `threads` mount holds and core 0 included, `n` places in turn from where the
/// previous mount stopped, so mounts spread over the shared cores instead of piling on the first.
/// A sequential mount stays on core 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    mounts: Vec<Vec<usize>>,
}

impl Layout {
    /// The placement of `mounts` over `count` cores.
    ///
    /// # Errors
    ///
    /// [`LayoutError`] when the `threads` mounts together ask for more than `count - 1` cores,
    /// naming each of them.
    pub fn plan(count: usize, mounts: &[Mount<'_>]) -> Result<Self, LayoutError> {
        let count = count.max(1);
        let threads = |mount: &&Mount<'_>| matches!(mount.workers.placement(), Placement::Threads);
        let asked: usize = mounts
            .iter()
            .filter(threads)
            .map(|mount| mount.workers.count().get())
            .sum();
        let available = count - 1;
        if asked > available {
            let mut named = String::new();
            for mount in mounts.iter().filter(threads) {
                if !named.is_empty() {
                    named.push_str(", ");
                }
                // Writing to a `String` cannot fail.
                let _ = write!(named, "`{}`: {}", mount.name, mount.workers.count());
            }
            return Err(LayoutError {
                asked,
                available,
                mounts: named,
            });
        }
        let shared: Vec<usize> = std::iter::once(0).chain(1 + asked..count).collect();
        let mut dedicated = 1;
        let mut cursor = 0;
        let placed = mounts
            .iter()
            .map(|mount| {
                let wanted = mount.workers.count().get();
                if mount.workers.is_sequential() {
                    vec![0]
                } else if threads(&mount) {
                    let held = (dedicated..dedicated + wanted).collect();
                    dedicated += wanted;
                    held
                } else {
                    (0..wanted)
                        .map(|_| {
                            let core = shared[cursor % shared.len()];
                            cursor += 1;
                            core
                        })
                        .collect()
                }
            })
            .collect();
        Ok(Self { mounts: placed })
    }

    /// The cores of mount `index`, one per worker or lane.
    ///
    /// # Panics
    ///
    /// When `index` is not a mount of the plan.
    #[must_use]
    pub fn cores(&self, index: usize) -> &[usize] {
        &self.mounts[index]
    }
}

/// The core a lane was to start on is gone: its mailbox is closed.
#[derive(Debug, Error)]
#[error("subscription `{subscription}` could not start a lane on core {core}: the core has ended")]
pub struct StartLaneError {
    subscription: String,
    core: usize,
}

/// What a core runs when its mailbox hands it a lane: it spawns the lane on the core's runtime.
type Job = Box<dyn FnOnce() -> JoinHandle<()> + Send>;

/// The mailboxes of the cores: how core 0 starts a lane on another core.
///
/// A mailbox is made at startup and used only then, once per lane, so a job is boxed; a delivery
/// never passes through it (deliveries travel the rings). A clone shares the mailboxes.
#[derive(Clone)]
pub struct Fabric {
    senders: Arc<[UnboundedSender<Job>]>,
    inboxes: Arc<Mutex<Vec<Option<UnboundedReceiver<Job>>>>>,
}

impl std::fmt::Debug for Fabric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fabric")
            .field("cores", &self.senders.len())
            .finish_non_exhaustive()
    }
}

impl Fabric {
    /// The mailboxes of `count` cores.
    #[must_use]
    pub fn open(count: usize) -> Self {
        let (senders, inboxes): (Vec<_>, Vec<_>) = (0..count.max(1))
            .map(|_| {
                let (sender, inbox) = mpsc::unbounded();
                (sender, Some(inbox))
            })
            .unzip();
        Self {
            senders: senders.into(),
            inboxes: Arc::new(Mutex::new(inboxes)),
        }
    }

    /// Asks core `core` to spawn what `job` returns, on the core's own runtime: the lane is built
    /// there and never leaves it.
    ///
    /// # Errors
    ///
    /// [`StartLaneError`] where the core's [`serve`](Self::serve) has ended, or never ran.
    pub fn run_on<Lane>(
        &self,
        core: usize,
        subscription: &str,
        job: Lane,
    ) -> Result<(), StartLaneError>
    where
        Lane: FnOnce() -> JoinHandle<()> + Send + 'static,
    {
        self.senders[core]
            .unbounded_send(Box::new(job))
            .map_err(|_| StartLaneError {
                subscription: subscription.to_owned(),
                core,
            })
    }

    /// Runs the lanes mounted on this core until `stop` is raised, then the ones already
    /// mailed, and returns once every lane has ended. Each core calls it once, as its work.
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: dropping it cancels the lanes this core runs.
    pub async fn serve(&self, core: usize, stop: &Stop) {
        let taken = self
            .inboxes
            .lock()
            .ok()
            .and_then(|mut all| all[core].take());
        let Some(mut inbox) = taken else {
            error!(target: "ruststream::dispatch", core, "the mailbox of the core is already served");
            return;
        };
        let mut lanes = FuturesUnordered::new();
        {
            let cancelled = stop.cancelled();
            pin_mut!(cancelled);
            loop {
                let next = inbox.next();
                pin_mut!(next);
                match future::select(next, cancelled.as_mut()).await {
                    Either::Left((Some(job), _)) => lanes.push(job()),
                    Either::Left((None, _)) | Either::Right(((), _)) => break,
                }
            }
        }
        // What was mailed before the stop is still started: its ring is drained by it, and a lane
        // that never started would leave the loop feeding it waiting.
        inbox.close();
        while let Some(job) = inbox.next().await {
            lanes.push(job());
        }
        while let Some(ended) = lanes.next().await {
            if ended.is_err() {
                error!(target: "ruststream::dispatch", core, "a lane panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    fn n(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("a count")
    }

    #[test]
    fn threads_take_the_cores_after_core_zero_and_workers_share_the_rest() {
        let layout = Layout::plan(
            5,
            &[
                Mount {
                    name: "scans",
                    workers: Workers::threads(n(2)),
                },
                Mount {
                    name: "orders",
                    workers: Workers::pool(n(3)),
                },
                Mount {
                    name: "audit",
                    workers: Workers::sequential(),
                },
            ],
        )
        .expect("fits");
        assert_eq!(layout.cores(0), [1, 2]);
        assert_eq!(layout.cores(1), [0, 3, 4]);
        assert_eq!(layout.cores(2), [0]);
    }

    #[test]
    fn mounts_continue_over_the_shared_cores_where_the_previous_stopped() {
        let layout = Layout::plan(
            3,
            &[
                Mount {
                    name: "a",
                    workers: Workers::pool(n(2)),
                },
                Mount {
                    name: "b",
                    workers: Workers::keyed(n(2)),
                },
            ],
        )
        .expect("fits");
        assert_eq!(layout.cores(0), [0, 1]);
        assert_eq!(layout.cores(1), [2, 0]);
    }

    #[test]
    fn threads_that_take_every_core_but_core_zero_fit() {
        let layout = Layout::plan(
            3,
            &[Mount {
                name: "scans",
                workers: Workers::threads(n(2)),
            }],
        )
        .expect("fits");
        assert_eq!(layout.cores(0), [1, 2]);
    }

    #[test]
    fn threads_that_need_core_zero_are_a_startup_error_naming_the_mounts() {
        let refused = Layout::plan(
            3,
            &[
                Mount {
                    name: "scans",
                    workers: Workers::threads(n(2)),
                },
                Mount {
                    name: "ocr",
                    workers: Workers::threads_keyed(n(1)),
                },
            ],
        )
        .expect_err("three dedicated cores on three cores");
        let said = refused.to_string();
        assert!(said.contains("`scans`: 2"), "{said}");
        assert!(said.contains("`ocr`: 1"), "{said}");
        assert!(
            said.contains("asks for 3") || said.contains("ask for 3"),
            "{said}"
        );
    }
}
