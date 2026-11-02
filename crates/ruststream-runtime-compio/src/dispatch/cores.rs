//! Which core each part of a subscription runs on: the placement table made at startup, and the
//! mailbox through which core 0 starts a lane on another core.

use std::fmt::{self, Debug, Formatter, Write};
use std::iter;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use compio::runtime::{JoinError, JoinHandle};
use event_listener::Event;
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, pin_mut, select};
use ruststream_runtime::{Halt, Placement, ShutdownWait, Workers};
use thiserror::Error;
use tracing::{debug, error};

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
        let shared: Vec<usize> = iter::once(0).chain(1 + asked..count).collect();
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
                    iter::repeat_with(|| {
                        let core = shared[cursor % shared.len()];
                        cursor += 1;
                        core
                    })
                    .take(wanted)
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

/// A lane to start: the subscription it belongs to, which a failure of the lane names, and the job
/// that spawns it.
struct Mail {
    subscription: Box<str>,
    job: Job,
}

/// The mailboxes of the cores: how core 0 starts a lane on another core.
///
/// A mailbox is made at startup and used only then, once per lane, so a job is boxed; a delivery
/// never passes through it (deliveries travel the rings). A clone shares the mailboxes.
#[derive(Clone)]
pub struct Fabric {
    senders: Arc<[UnboundedSender<Mail>]>,
    inboxes: Arc<Mutex<Vec<Option<UnboundedReceiver<Mail>>>>>,
    lanes: Arc<Lanes>,
}

/// How many lanes exist, on any core, and the wait for the last one to be gone.
#[derive(Debug, Default)]
struct Lanes {
    alive: AtomicUsize,
    gone: Event,
    /// Set once the teardown has closed the mailboxes no core had started serving.
    unserved_closed: AtomicBool,
}

/// A lane's place in the count of live lanes, held by the lane for as long as it exists: it is
/// released when the lane ends, is cancelled with its core, or is dropped unstarted.
#[derive(Debug)]
pub struct LaneGuard(Arc<Lanes>);

impl Drop for LaneGuard {
    fn drop(&mut self) {
        if self.0.alive.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.gone.notify(usize::MAX);
        }
    }
}

impl Debug for Fabric {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fabric")
            .field("cores", &self.senders.len())
            .finish_non_exhaustive()
    }
}

impl Fabric {
    /// The mailboxes of `count` cores.
    #[must_use]
    pub fn open(count: usize) -> Self {
        let (senders, inboxes): (Vec<_>, Vec<_>) = iter::repeat_with(|| {
            let (sender, inbox) = mpsc::unbounded();
            (sender, Some(inbox))
        })
        .take(count.max(1))
        .unzip();
        Self {
            senders: senders.into(),
            inboxes: Arc::new(Mutex::new(inboxes)),
            lanes: Arc::default(),
        }
    }

    /// A place in the count of live lanes, for a lane about to be started: the lane holds it for
    /// its whole life.
    #[must_use]
    pub fn lane_guard(&self) -> LaneGuard {
        self.lanes.alive.fetch_add(1, Ordering::AcqRel);
        LaneGuard(Arc::clone(&self.lanes))
    }

    /// Resolves once no lane exists on any core: every handler a lane ran is finished or dropped.
    ///
    /// A lane cancelled at the shutdown cap is gone only when its own core has dropped it, which
    /// this waits for. The mailbox of a core that has not started serving is closed first, with
    /// the lanes mailed to it: called at teardown, after the loops that feed the lanes are over,
    /// a core that never started would otherwise hold the wait for good.
    pub async fn lanes_gone(&self) {
        self.close_unserved();
        loop {
            if self.lanes.alive.load(Ordering::Acquire) == 0 {
                return;
            }
            let listener = self.lanes.gone.listen();
            if self.lanes.alive.load(Ordering::Acquire) == 0 {
                return;
            }
            listener.await;
        }
    }

    /// Closes the mailboxes no core has taken, which drops the lanes mailed to them.
    fn close_unserved(&self) {
        if let Ok(mut inboxes) = self.inboxes.lock() {
            self.lanes.unserved_closed.store(true, Ordering::Release);
            inboxes.iter_mut().for_each(|inbox| drop(inbox.take()));
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
            .unbounded_send(Mail {
                subscription: subscription.into(),
                job: Box::new(job),
            })
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
            if self.lanes.unserved_closed.load(Ordering::Acquire) {
                // The teardown got here first, and this core's lanes went with the mailbox.
                debug!(target: "ruststream::dispatch", core, "the mailbox was closed before the core served it");
                return;
            }
            error!(target: "ruststream::dispatch", core, "the mailbox of the core is already served");
            return;
        };
        let mut lanes = FuturesUnordered::new();
        let ended = |subscription: &str, outcome: Result<(), JoinError>| {
            if outcome.is_err() {
                error!(target: "ruststream::dispatch", core, subscription, "a lane panicked");
                // A lane that is gone leaves the subscription on fewer lanes than it asked for,
                // so the service fails like it does for a handler that panics.
                stop.fail(subscription, &format!("a lane panicked on core {core}"));
            }
        };
        {
            let cancelled = stop.cancelled().fuse();
            pin_mut!(cancelled);
            loop {
                select! {
                    mail = inbox.next() => match mail {
                        Some(mail) => lanes.push(watch(mail)),
                        None => break,
                    },
                    (subscription, outcome) = lanes.select_next_some() => {
                        ended(&subscription, outcome);
                    }
                    () = cancelled => break,
                }
            }
        }
        // What was mailed before the stop is still started: its ring is drained by it, and a lane
        // that never started would leave the loop feeding it waiting.
        inbox.close();
        while let Some(mail) = inbox.next().await {
            lanes.push(watch(mail));
        }
        while let Some((subscription, outcome)) = lanes.next().await {
            ended(&subscription, outcome);
        }
    }
}

/// Starts the lane of `mail` on this core and resolves with how it ended, named by its
/// subscription.
fn watch(mail: Mail) -> impl Future<Output = (Box<str>, Result<(), JoinError>)> {
    let Mail { subscription, job } = mail;
    // Spawned now, not when the result is first polled: the lane's ring is waiting for it.
    let lane = job();
    async move { (subscription, lane.await) }
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
