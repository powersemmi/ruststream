//! The launcher: core 0 on the calling thread, one thread per further core, each with a compio
//! runtime of its own.

use std::collections::HashSet;
use std::error::Error;
use std::io;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::pin;
use std::thread::{self, ScopedJoinHandle};

use compio::runtime::RuntimeBuilder;
use compio::signal::ctrl_c;
#[cfg(unix)]
use compio::signal::unix::signal as unix_signal;
use futures::future;
use ruststream_runtime::{Core, Halt};
use thiserror::Error;
use tracing::{error, warn};

use crate::Stop;
use crate::spawn::spawn;

type BoxError = Box<dyn Error + Send + Sync>;

/// The number of the termination signal on Unix.
#[cfg(unix)]
const SIGTERM: i32 = 15;

/// Why a run of the cores ended other than on request.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoresError {
    /// The runtime of a core could not be built.
    #[error("core {core}: the runtime could not be built")]
    Runtime {
        /// The core whose runtime failed.
        core: usize,
        /// The cause.
        #[source]
        source: io::Error,
    },
    /// The thread of a core could not be started.
    #[error("core {core}: the thread could not be started")]
    Thread {
        /// The core whose thread failed.
        core: usize,
        /// The cause.
        #[source]
        source: io::Error,
    },
    /// The work of a core ended with an error.
    #[error("core {core} failed")]
    Core {
        /// The core that failed.
        core: usize,
        /// The cause.
        #[source]
        source: BoxError,
    },
    /// The work of a core panicked.
    #[error("core {core} panicked")]
    Panicked {
        /// The core that panicked.
        core: usize,
    },
}

/// Where each core runs, and what the launcher says about it.
#[derive(Debug, PartialEq, Eq)]
struct Pinning {
    /// The CPU of each core; `None` where the core is not pinned.
    cpus: Vec<Option<usize>>,
    /// Said once at startup, where the plan is not what was asked for.
    note: Option<&'static str>,
}

impl Pinning {
    /// Core `i` takes the `i`-th CPU the process may run on, wrapping when there are fewer CPUs
    /// than cores; nothing is pinned where the allowed set is unknown or empty.
    fn plan(count: usize, allowed: Option<&[usize]>) -> Self {
        match allowed {
            None | Some([]) => Self {
                cpus: vec![None; count],
                note: Some(
                    "the CPUs this process may use are unknown, so the cores are not pinned",
                ),
            },
            Some(allowed) => Self {
                cpus: (0..count)
                    .map(|core| Some(allowed[core % allowed.len()]))
                    .collect(),
                note: (count > allowed.len())
                    .then_some("there are more cores than allowed CPUs, so cores share a CPU"),
            },
        }
    }

    fn unpinned(count: usize) -> Self {
        Self {
            cpus: vec![None; count],
            note: None,
        }
    }
}

/// The launcher of the compio runner's cores.
///
/// Core 0 is the calling thread and owns the brokers; each further core is a thread with a runtime
/// of its own. A core is pinned to one CPU of the set the process may run on; where that set is
/// unknown or empty (a restricted cgroup that hides it), the cores run unpinned and the launcher
/// says so once.
///
/// # Examples
///
/// ```
/// use std::convert::Infallible;
/// use std::num::NonZeroUsize;
///
/// use ruststream_runtime::ShutdownWait;
/// use ruststream_runtime_compio::{Cores, Stop};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let cores = Cores::new(NonZeroUsize::new(2).ok_or("no cores")?).without_signals();
/// let ends = cores.run(&Stop::default(), async |core, stop| {
///     if core.is_main() {
///         // The main core serves, then stops the others.
///         stop.cancel();
///     } else {
///         stop.cancelled().await;
///     }
///     Ok::<_, Infallible>(core.index())
/// })?;
/// assert_eq!(ends, [0, 1]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cores {
    count: NonZeroUsize,
    pinned: bool,
    signals: bool,
}

impl Cores {
    /// `count` cores, pinned, with the process's termination signals ending the run.
    #[must_use]
    pub const fn new(count: NonZeroUsize) -> Self {
        Self {
            count,
            pinned: true,
            signals: true,
        }
    }

    /// One core per CPU the process may use.
    #[must_use]
    pub fn available() -> Self {
        Self::new(thread::available_parallelism().unwrap_or(NonZeroUsize::MIN))
    }

    /// How many cores run.
    #[must_use]
    pub const fn count(self) -> NonZeroUsize {
        self.count
    }

    /// Leaves the threads where the operating system puts them.
    #[must_use]
    pub const fn unpinned(mut self) -> Self {
        self.pinned = false;
        self
    }

    /// Leaves the process's signals to the host: the run ends only through the [`Stop`].
    #[must_use]
    pub const fn without_signals(mut self) -> Self {
        self.signals = false;
        self
    }

    /// Runs `make(core, stop)` on every core and returns what each core's future produced, in core
    /// order, once all of them have ended.
    ///
    /// Core 0 runs on the calling thread. A core whose future fails, panics or whose runtime
    /// cannot be built raises `stop` with the failure, so the others end; a termination signal
    /// raises it without one. When core 0's future ends, `stop` is raised too. Each core's future
    /// is expected to end once `stop` is raised. Of several failures, the one of the lowest core
    /// is returned and the others are logged.
    ///
    /// # Errors
    ///
    /// The first [`CoresError`], by core.
    ///
    /// # Panics
    ///
    /// Resumes the panic of core 0, after the other cores have ended.
    pub fn run<Make, Out, Failure>(self, stop: &Stop, make: Make) -> Result<Vec<Out>, CoresError>
    where
        Make: AsyncFn(Core, Stop) -> Result<Out, Failure> + Sync,
        Out: Send,
        Failure: Into<BoxError>,
    {
        let count = self.count.get();
        let pinning = if self.pinned {
            let allowed: Option<Vec<usize>> =
                core_affinity::get_core_ids().map(|ids| ids.into_iter().map(|id| id.id).collect());
            let plan = Pinning::plan(count, allowed.as_deref());
            if let Some(note) = plan.note {
                warn!(target: "ruststream::cores", cores = count, "{note}");
            }
            plan
        } else {
            Pinning::unpinned(count)
        };

        let make = &make;
        let pinning = &pinning;
        thread::scope(|scope| {
            let mut spawned: Vec<(usize, ScopedJoinHandle<'_, Result<Out, CoresError>>)> =
                Vec::new();
            let mut results: Vec<Result<Out, CoresError>> = Vec::with_capacity(count);
            for index in 1..count {
                let core_stop = stop.clone();
                let started = thread::Builder::new()
                    .name(format!("ruststream-core-{index}"))
                    .spawn_scoped(scope, move || {
                        run_core(
                            Core::new(index, self.count),
                            pinning,
                            &core_stop,
                            make,
                            false,
                        )
                    });
                match started {
                    Ok(handle) => spawned.push((index, handle)),
                    Err(source) => {
                        stop.fail(
                            "cores",
                            &format!("core {index}: the thread could not be started"),
                        );
                        results.push(Err(CoresError::Thread {
                            core: index,
                            source,
                        }));
                        break;
                    }
                }
            }

            let main = catch_unwind(AssertUnwindSafe(|| {
                run_core(Core::new(0, self.count), pinning, stop, make, self.signals)
            }));
            // Whatever core 0 did, the others must be told to end before they are joined.
            stop.cancel();

            let mut ordered: Vec<Result<Out, CoresError>> = Vec::with_capacity(count);
            match main {
                Ok(main) => ordered.push(main),
                Err(payload) => {
                    for (_, handle) in spawned {
                        let _ = handle.join();
                    }
                    resume_unwind(payload);
                }
            }
            for (index, handle) in spawned {
                ordered.push(
                    handle
                        .join()
                        .unwrap_or(Err(CoresError::Panicked { core: index })),
                );
            }
            ordered.extend(results);
            collect(ordered)
        })
    }
}

/// The outputs in core order, or the error of the lowest core, the rest logged.
fn collect<Out>(ordered: Vec<Result<Out, CoresError>>) -> Result<Vec<Out>, CoresError> {
    let mut outputs = Vec::with_capacity(ordered.len());
    let mut first: Option<CoresError> = None;
    for result in ordered {
        match result {
            Ok(output) => outputs.push(output),
            Err(failure) if first.is_none() => first = Some(failure),
            Err(failure) => error!(target: "ruststream::cores", %failure, "a further core failed"),
        }
    }
    first.map_or(Ok(outputs), Err)
}

/// One core: its runtime, pinned and made current, running the core's future to its end.
fn run_core<Make, Out, Failure>(
    core: Core,
    pinning: &Pinning,
    stop: &Stop,
    make: &Make,
    signals: bool,
) -> Result<Out, CoresError>
where
    Make: AsyncFn(Core, Stop) -> Result<Out, Failure>,
    Failure: Into<BoxError>,
{
    let index = core.index();
    let mut builder = RuntimeBuilder::new();
    if let Some(cpu) = pinning.cpus[index] {
        builder.thread_affinity(HashSet::from([cpu]));
    }
    let runtime = builder.build().map_err(|source| {
        stop.fail(
            "cores",
            &format!("core {index}: the runtime could not be built"),
        );
        CoresError::Runtime {
            core: index,
            source,
        }
    })?;

    let _core = core.enter();
    let ended = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            if signals {
                spawn(raise_on_signal(stop.clone())).detach();
            }
            make(core, stop.clone()).await
        })
    }));
    match ended {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(source)) => {
            let source = source.into();
            stop.fail("cores", &format!("core {index}: {source}"));
            Err(CoresError::Core {
                core: index,
                source,
            })
        }
        Err(payload) => {
            stop.fail("cores", &format!("core {index} panicked"));
            if index == 0 {
                resume_unwind(payload);
            }
            Err(CoresError::Panicked { core: index })
        }
    }
}

/// Raises `stop` when the process is asked to end: Ctrl-C everywhere, and SIGTERM on Unix.
async fn raise_on_signal(stop: Stop) {
    let interrupt = pin!(ctrl_c());
    #[cfg(unix)]
    let ended = {
        let terminate = pin!(unix_signal(SIGTERM));
        future::select(interrupt, terminate).await.factor_first().0
    };
    #[cfg(not(unix))]
    let ended = interrupt.await;
    if let Err(failure) = ended {
        warn!(target: "ruststream::cores", %failure, "the process's signals could not be watched");
        return;
    }
    stop.cancel();
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use compio::driver::{DriverType, ProactorBuilder};
    use compio::runtime::RuntimeBuilder;
    use compio::time::sleep;

    use super::Pinning;

    // A host that refuses an io_uring (a container profile that blocks it, a kernel without it,
    // no memory for the ring) is served by the polling driver, so the driver must be compiled in.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_polling_driver_is_there_for_a_host_that_refuses_io_uring() {
        let mut proactor = ProactorBuilder::new();
        proactor.driver_type(DriverType::Poll);
        let runtime = RuntimeBuilder::new()
            .with_proactor(proactor)
            .build()
            .expect("the runtime builds");
        assert_eq!(runtime.driver_type(), DriverType::Poll);
        runtime.block_on(async { sleep(Duration::from_millis(1)).await });
    }

    #[test]
    fn each_core_takes_its_own_allowed_cpu() {
        let plan = Pinning::plan(3, Some(&[4, 5, 6, 7]));
        assert_eq!(plan.cpus, [Some(4), Some(5), Some(6)]);
        assert_eq!(plan.note, None);
    }

    #[test]
    fn more_cores_than_cpus_share_and_say_so() {
        let plan = Pinning::plan(3, Some(&[2, 3]));
        assert_eq!(plan.cpus, [Some(2), Some(3), Some(2)]);
        assert!(plan.note.is_some());
    }

    #[test]
    fn an_empty_or_unknown_allowed_set_leaves_the_cores_unpinned_and_says_so() {
        for allowed in [None, Some(&[][..])] {
            let plan = Pinning::plan(2, allowed);
            assert_eq!(plan.cpus, [None, None]);
            assert!(plan.note.is_some());
        }
    }
}
