//! The virtual clock a test moves: timers complete only when `advance` reaches them.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use compio::runtime::{self, Runtime};
use ruststream_runtime::Clock;
use ruststream_runtime_compio::testing::VirtualClock;
use ruststream_runtime_compio::{CompioClock, time, yield_now};

/// Runs `test` on a fresh compio runtime with the virtual clock held.
fn on_virtual_clock<Test>(test: impl FnOnce(VirtualClock) -> Test)
where
    Test: Future<Output = ()>,
{
    let runtime = Runtime::new().expect("a compio runtime");
    let clock = VirtualClock::take();
    runtime.block_on(test(clock));
}

/// Spawns a task that sleeps `by` and raises the returned flag.
fn sleeper(by: Duration) -> Rc<Cell<bool>> {
    let woke = Rc::new(Cell::new(false));
    let flag = Rc::clone(&woke);
    runtime::spawn(async move {
        time::sleep(by).await;
        flag.set(true);
    })
    .detach();
    woke
}

#[test]
fn a_sleep_completes_only_after_the_clock_reaches_it() {
    on_virtual_clock(async |clock| {
        let woke = sleeper(Duration::from_secs(5));
        yield_now().await;
        assert!(!woke.get(), "the sleep ended before any time passed");

        clock.advance(Duration::from_secs(4));
        yield_now().await;
        assert!(!woke.get(), "the sleep ended a second early");

        clock.advance(Duration::from_secs(1));
        yield_now().await;
        assert!(woke.get(), "the sleep did not end at its deadline");
    });
}

#[test]
fn now_moves_by_what_the_test_advanced() {
    on_virtual_clock(async |clock| {
        let started = CompioClock.now();
        assert_eq!(
            CompioClock.now(),
            started,
            "time stands still between advances"
        );

        clock.advance(Duration::from_secs(7));
        assert_eq!(CompioClock.now() - started, Duration::from_secs(7));
        assert_eq!(clock.now(), CompioClock.now());
    });
}

#[test]
fn the_clock_sleep_and_sleep_until_honour_the_virtual_time() {
    on_virtual_clock(async |clock| {
        let woke = Rc::new(Cell::new(0_u8));
        let deadline = CompioClock.now() + Duration::from_secs(3);
        for timer in [
            Box::pin(CompioClock.sleep(Duration::from_secs(3))),
            Box::pin(CompioClock.sleep_until(deadline)),
        ] {
            let woke = Rc::clone(&woke);
            runtime::spawn(async move {
                timer.await;
                woke.set(woke.get() + 1);
            })
            .detach();
        }
        yield_now().await;
        assert_eq!(woke.get(), 0);

        clock.advance(Duration::from_secs(3));
        yield_now().await;
        assert_eq!(woke.get(), 2);
    });
}

#[test]
fn a_deadline_already_passed_completes_at_once() {
    on_virtual_clock(async |clock| {
        clock.advance(Duration::from_secs(1));
        let passed = CompioClock
            .now()
            .checked_sub(Duration::from_millis(1))
            .expect("an instant a millisecond back");
        CompioClock.sleep_until(passed).await;
        time::sleep(Duration::ZERO).await;
    });
}

#[test]
fn advance_to_next_jumps_to_the_nearest_deadline() {
    on_virtual_clock(async |clock| {
        assert!(!clock.advance_to_next(), "no timer is armed");

        let started = clock.now();
        let near = sleeper(Duration::from_secs(2));
        let far = sleeper(Duration::from_secs(9));
        yield_now().await;

        assert!(clock.advance_to_next());
        yield_now().await;
        assert_eq!(clock.now() - started, Duration::from_secs(2));
        assert!(near.get());
        assert!(!far.get());

        assert!(clock.advance_to_next());
        yield_now().await;
        assert_eq!(clock.now() - started, Duration::from_secs(9));
        assert!(far.get());
        assert!(!clock.advance_to_next());
    });
}

#[test]
fn a_dropped_sleep_leaves_no_timer_behind() {
    on_virtual_clock(async |clock| {
        let pending = Box::pin(time::sleep(Duration::from_secs(60)));
        let task = runtime::spawn(pending);
        yield_now().await;
        drop(task);
        yield_now().await;
        assert!(
            !clock.advance_to_next(),
            "the cancelled sleep still held a timer"
        );
    });
}

#[test]
fn without_a_lease_the_wall_clock_runs() {
    let runtime = Runtime::new().expect("a compio runtime");
    runtime.block_on(async {
        let started = Instant::now();
        time::sleep(Duration::from_millis(20)).await;
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert!(CompioClock.now() >= started);
    });
}

#[test]
#[should_panic(expected = "already held on this thread")]
fn a_second_lease_on_one_thread_is_refused() {
    let _first = VirtualClock::take();
    let _second = VirtualClock::take();
}
