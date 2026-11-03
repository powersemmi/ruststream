//! `Cores::run`: the number of cores, what each one sees, and how the first failure ends them all.

use std::convert::Infallible;
#[cfg(target_os = "linux")]
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, ThreadId};

use futures::StreamExt;
use futures::channel::mpsc::{self, UnboundedReceiver};
use ruststream_runtime::{Core, ShutdownWait};
use ruststream_runtime_compio::{Cores, CoresError, Stop};

fn cores(count: usize) -> Cores {
    Cores::new(NonZeroUsize::new(count).expect("a core count")).without_signals()
}

/// What a core reports about itself.
#[derive(Debug)]
struct Seen {
    index: usize,
    count: usize,
    current: Core,
    thread: ThreadId,
    #[cfg(target_os = "linux")]
    pinned_to: Option<String>,
}

/// The CPUs this thread may run on, where the platform says.
#[cfg(target_os = "linux")]
fn allowed_cpus() -> Option<String> {
    let status = fs::read_to_string("/proc/thread-self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .map(|list| list.trim().to_owned())
}

/// Runs `launcher` with every core reporting itself; core 0 ends the run once all have reported.
fn report_all(launcher: Cores) -> Vec<Seen> {
    let (report, collector) = mpsc::unbounded::<Seen>();
    let collector = Mutex::new(Some(collector));
    launcher
        .run(&Stop::default(), async move |core, stop| {
            report
                .unbounded_send(Seen {
                    index: core.index(),
                    count: core.count().get(),
                    current: Core::current(),
                    thread: thread::current().id(),
                    #[cfg(target_os = "linux")]
                    pinned_to: allowed_cpus(),
                })
                .expect("the collector listens");
            let collector: Option<UnboundedReceiver<Seen>> = if core.is_main() {
                collector.lock().expect("lock").take()
            } else {
                None
            };
            let Some(mut collector) = collector else {
                stop.cancelled().await;
                return Ok::<_, Infallible>(Vec::new());
            };
            let mut seen = Vec::new();
            while seen.len() < core.count().get() {
                seen.push(collector.next().await.expect("every core reports"));
            }
            stop.cancel();
            Ok(seen)
        })
        .expect("a clean stop")
        .into_iter()
        .flatten()
        .collect()
}

#[test]
fn every_core_runs_on_a_thread_of_its_own_and_knows_its_place() {
    let caller = thread::current().id();

    let mut seen = report_all(cores(3).unpinned());

    seen.sort_by_key(|one| one.index);
    assert_eq!(
        seen.iter().map(|one| one.index).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    for one in &seen {
        assert_eq!(one.count, 3);
        assert_eq!(
            one.current.index(),
            one.index,
            "Core::current is the core's own"
        );
        assert_eq!(one.current.count().get(), 3);
    }
    assert_eq!(seen[0].thread, caller, "core 0 is the calling thread");
    assert_ne!(seen[1].thread, caller);
    assert_ne!(seen[2].thread, caller);
    assert_ne!(seen[1].thread, seen[2].thread);
}

#[test]
fn one_core_runs_on_the_calling_thread_alone() {
    let seen = report_all(cores(1).unpinned());
    assert_eq!(seen.len(), 1);
    assert_eq!((seen[0].index, seen[0].count), (0, 1));
}

#[cfg(target_os = "linux")]
#[test]
fn a_pinned_core_may_run_on_one_cpu_only() {
    let seen = report_all(cores(2));
    for one in &seen {
        let Some(cpus) = &one.pinned_to else { continue };
        assert!(
            !cpus.contains(',') && !cpus.contains('-'),
            "core {} may run on {cpus}, not on one CPU",
            one.index
        );
    }
}

#[test]
fn a_failing_core_stops_the_others_and_is_the_error_returned() {
    let outcome = cores(3)
        .unpinned()
        .run(&Stop::default(), async |core, stop| {
            if core.index() == 2 {
                return Err(io::Error::other("the broker refused"));
            }
            stop.cancelled().await;
            Ok(())
        });

    let Err(CoresError::Core { core, source }) = outcome else {
        panic!("expected the failure of core 2, got {outcome:?}");
    };
    assert_eq!(core, 2);
    assert_eq!(source.to_string(), "the broker refused");
}

#[test]
fn a_failing_main_core_stops_the_others() {
    let outcome = cores(3)
        .unpinned()
        .run(&Stop::default(), async |core, stop| {
            if core.is_main() {
                return Err(io::Error::other("startup failed"));
            }
            stop.cancelled().await;
            Ok(())
        });

    assert!(
        matches!(outcome, Err(CoresError::Core { core: 0, .. })),
        "{outcome:?}"
    );
}

#[test]
fn the_error_of_the_lowest_core_is_the_one_returned() {
    let outcome = cores(3)
        .unpinned()
        .run(&Stop::default(), async |core, _stop| {
            Err::<(), _>(io::Error::other(format!("core {}", core.index())))
        });

    let Err(CoresError::Core { core, source }) = outcome else {
        panic!("expected a core failure, got {outcome:?}");
    };
    assert_eq!((core, source.to_string().as_str()), (0, "core 0"));
}

#[test]
fn a_panicking_core_is_reported_and_the_others_still_stop() {
    let outcome = cores(2)
        .unpinned()
        .run(&Stop::default(), async |core, stop| {
            assert!(core.index() != 1, "the handler on core 1 panicked");
            stop.cancelled().await;
            Ok::<(), Infallible>(())
        });

    assert!(
        matches!(outcome, Err(CoresError::Panicked { core: 1 })),
        "{outcome:?}"
    );
}

#[test]
fn a_run_that_ends_cleanly_returns_ok_with_the_stop_raised() {
    let stop = Stop::default();
    let outcome = cores(2).unpinned().run(&stop, async |core, stop| {
        if core.is_main() {
            stop.cancel();
        } else {
            stop.cancelled().await;
        }
        Ok::<(), Infallible>(())
    });
    outcome.expect("a clean stop");
    assert!(stop.is_cancelled());
}

#[test]
fn the_cores_are_as_many_as_the_process_may_use() {
    let available = Cores::available();
    assert_eq!(
        available.count(),
        thread::available_parallelism().expect("the host reports its CPUs")
    );
}

#[test]
fn a_panicking_main_core_stops_the_others_and_the_panic_goes_on() {
    let stop = Stop::default();
    let others_stopped = AtomicBool::new(false);
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        cores(3).unpinned().run(&stop, async |core, stop| {
            assert!(!core.is_main(), "the main core broke");
            stop.cancelled().await;
            others_stopped.store(true, Ordering::SeqCst);
            Ok::<(), Infallible>(())
        })
    }));

    let payload = unwound.expect_err("the panic of core 0 reaches the caller");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"the main core broke"));
    assert!(stop.is_cancelled());
    assert!(
        others_stopped.load(Ordering::SeqCst),
        "the other cores were stopped and joined first"
    );
}
