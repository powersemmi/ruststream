//! `Tasks`: detached work runs on the core that detached it and is counted until it ends.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream_runtime::Detach;
use ruststream_runtime_compio::testing::VirtualClock;
use ruststream_runtime_compio::{Tasks, yield_now};

fn on_virtual_clock(test: impl FnOnce(VirtualClock) -> Box<dyn Future<Output = ()>>) {
    let runtime = Runtime::new().expect("a compio runtime");
    let clock = VirtualClock::take();
    runtime.block_on(Box::into_pin(test(clock)));
}

#[test]
fn detached_work_runs_and_is_counted_until_it_ends() {
    on_virtual_clock(|_clock| {
        Box::new(async {
            let tasks = Tasks::default();
            let ran = Arc::new(AtomicBool::new(false));
            let flag = Arc::clone(&ran);
            tasks.detach(async move { flag.store(true, Ordering::SeqCst) });
            assert_eq!(tasks.pending(), 1, "the work is counted from the call");

            tasks.drained().await;

            assert!(ran.load(Ordering::SeqCst));
            assert_eq!(tasks.pending(), 0);
        })
    });
}

#[test]
fn drained_waits_for_work_that_is_still_running() {
    on_virtual_clock(|clock| {
        Box::new(async move {
            let tasks = Tasks::default();
            tasks.detach_after(Duration::from_secs(5), async {});
            let mut drained = Box::pin(tasks.drained());
            assert!(futures::poll!(drained.as_mut()).is_pending());

            clock.advance(Duration::from_secs(5));
            drained.await;
            assert_eq!(tasks.pending(), 0);
        })
    });
}

#[test]
fn delayed_work_is_built_once_its_delay_has_passed() {
    on_virtual_clock(|clock| {
        Box::new(async move {
            let tasks = Tasks::default();
            let built = Arc::new(AtomicBool::new(false));
            let seen = Arc::clone(&built);
            tasks.detach_after_with(Duration::from_secs(5), move || {
                seen.store(true, Ordering::SeqCst);
                async {}
            });

            clock.advance(Duration::from_secs(4));
            yield_now().await;
            assert!(!built.load(Ordering::SeqCst), "built before its delay");

            clock.advance(Duration::from_secs(1));
            tasks.drained().await;
            assert!(built.load(Ordering::SeqCst));
        })
    });
}

#[test]
fn a_panicking_task_is_still_taken_off_the_count() {
    on_virtual_clock(|_clock| {
        Box::new(async {
            let tasks = Tasks::default();
            tasks.detach(async { panic!("the continuation failed") });
            tasks.drained().await;
            assert_eq!(tasks.pending(), 0);
        })
    });
}
