//! The sleep on the wall clock: `Send` however far it has got, and no allocation per timer.

use std::future::poll_fn;
use std::iter;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use allocation_counter::measure;
use compio::driver::DriverType;
use compio::runtime::{Runtime, spawn};
use compio::time::sleep as compio_sleep;
use futures::task::noop_waker_ref;
use ruststream_runtime::Clock;
use ruststream_runtime_compio::time::{self, Sleep};
use ruststream_runtime_compio::{CompioClock, test_runtime};

const DELAY: Duration = Duration::from_millis(30);

/// What a sleep awaited in the future a runtime blocks on may cost beyond compio's own timer on
/// compio's polling driver (macOS, or a Linux host without `io_uring`). The wheel's task wakes that
/// future, so the runtime waits once more, with no timer pending, before the future arms its next
/// sleep; the polling driver builds a new event list after a wait without a deadline
/// (<https://github.com/compio-rs/compio/issues/1107>). A sleep in a spawned task arms the next
/// one in the same turn and pays nothing.
const POLLING_EXTRA_PER_SLEEP: u64 = 1;

/// A sleep of the measured kind, boxed so both kinds run through the same probe.
type MakeSleep = fn(Duration) -> Pin<Box<dyn Future<Output = ()>>>;

/// Where the sleeps of a measured run are awaited.
#[derive(Clone, Copy)]
enum Awaited {
    /// In the future the runtime blocks on.
    InBlockOn,
    /// In a task spawned on the runtime, where a handler's sleeps are awaited.
    InSpawnedTask,
}

fn assert_send<Value: Send>() {}

fn poll_once(sleep: &mut Sleep) -> Poll<()> {
    Pin::new(sleep).poll(&mut Context::from_waker(noop_waker_ref()))
}

#[test]
fn a_sleep_is_send() {
    assert_send::<Sleep>();
    assert_send::<<CompioClock as Clock>::Sleep>();
}

#[test]
fn a_sleep_made_on_one_thread_completes_on_the_core_that_polls_it() {
    let sleep = time::sleep(DELAY);
    let started = Instant::now();
    thread::spawn(move || {
        test_runtime().expect("a compio runtime").block_on(sleep);
    })
    .join()
    .expect("the core ended");
    assert!(started.elapsed() >= DELAY, "the sleep ended early");
}

#[test]
fn a_sleep_armed_on_one_core_completes_on_another() {
    let (to_other, from_first) = mpsc::channel::<Sleep>();
    let (armed, wait_armed) = mpsc::channel::<()>();
    let (done, wait_done) = mpsc::channel::<()>();
    let started = Instant::now();

    let first = thread::spawn(move || {
        test_runtime()
            .expect("a compio runtime")
            .block_on(async move {
                let mut sleep = time::sleep(DELAY);
                assert!(poll_once(&mut sleep).is_pending());
                to_other.send(sleep).expect("the other core is up");
                armed.send(()).expect("the test waits");
                // The core that armed the timer keeps running, as a core does.
                while wait_done.try_recv().is_err() {
                    time::sleep(Duration::from_millis(1)).await;
                }
            });
    });
    wait_armed.recv().expect("the first core armed its sleep");
    let sleep = from_first.recv().expect("the sleep was handed over");
    test_runtime().expect("a compio runtime").block_on(sleep);
    assert!(started.elapsed() >= DELAY, "the sleep ended early");
    done.send(()).expect("the first core waits");
    first.join().expect("the first core ended");
}

#[test]
fn a_sleep_outlives_the_runtime_that_armed_it() {
    let mut sleep = time::sleep(DELAY);
    let started = Instant::now();
    {
        let runtime = test_runtime().expect("a compio runtime");
        runtime.block_on(async {
            assert!(poll_once(&mut sleep).is_pending());
        });
    }
    test_runtime().expect("a compio runtime").block_on(sleep);
    assert!(started.elapsed() >= DELAY, "the sleep ended early");
}

#[test]
fn a_dropped_sleep_does_not_hold_back_the_next() {
    test_runtime().expect("a compio runtime").block_on(async {
        let mut dropped = time::sleep(Duration::from_millis(5));
        assert!(poll_once(&mut dropped).is_pending());
        drop(dropped);
        let started = Instant::now();
        time::sleep(Duration::from_millis(10)).await;
        assert!(started.elapsed() >= Duration::from_millis(10));
    });
}

/// Runs `arm` once to size what the wheel keeps, then again under the counter.
fn allocations_after_warm_up(mut arm: impl FnMut()) -> u64 {
    // Each round leaves cancelled entries behind, so the heap reaches its size after a few.
    for _ in 0..3 {
        arm();
    }
    measure(arm).count_total
}

#[test]
fn timers_armed_together_cost_no_allocation_once_the_wheel_has_grown() {
    const COUNT: usize = 1000;
    test_runtime().expect("a compio runtime").block_on(async {
        let deadline = Instant::now() + DELAY;
        let mut sleeps: Vec<Sleep> = iter::repeat_with(|| CompioClock.sleep_until(deadline))
            .take(COUNT)
            .collect();
        let allocated = allocations_after_warm_up(|| {
            for sleep in &mut sleeps {
                assert!(poll_once(sleep).is_pending());
            }
            // Dropping and making them again frees every slot for the next round.
            for sleep in &mut sleeps {
                *sleep = CompioClock.sleep_until(deadline);
            }
        });
        assert_eq!(allocated, 0, "arming {COUNT} sleeps allocated");
    });
}

/// Allocations of `count` sleeps in turn, one by one, as `sleep` makes them, awaited where
/// `awaited` says.
fn sleeps_in_turn(runtime: &Runtime, count: u32, sleep: MakeSleep, awaited: Awaited) -> u64 {
    measure(|| {
        runtime.block_on(async {
            let sleeps = async move {
                for _ in 0..count {
                    sleep(Duration::from_micros(20)).await;
                }
            };
            match awaited {
                Awaited::InBlockOn => sleeps.await,
                Awaited::InSpawnedTask => spawn(sleeps).await.expect("the sleeps end"),
            }
        });
    })
    .count_total
}

/// What 1000 sleeps in turn allocate, awaited where `awaited` says: ours, then compio's own.
fn a_thousand_sleeps(runtime: &Runtime, awaited: Awaited) -> (u64, u64) {
    // The box is the probe's own, so it is subtracted by running both sides through it.
    let ours: MakeSleep = |delay| Box::pin(time::sleep(delay));
    let compio: MakeSleep = |delay| Box::pin(compio_sleep(delay));
    let cost = |count, sleep| sleeps_in_turn(runtime, count, sleep, awaited);
    cost(1, ours);
    cost(1, compio);
    (
        cost(1010, ours) - cost(10, ours),
        cost(1010, compio) - cost(10, compio),
    )
}

#[test]
fn a_sleep_in_turn_costs_what_one_timer_of_compio_costs() {
    let runtime = test_runtime().expect("a compio runtime");
    let (ours_per_sleep, compio_per_sleep) = a_thousand_sleeps(&runtime, Awaited::InBlockOn);
    let driver = runtime.driver_type();
    let polling_extra = if driver == DriverType::Poll {
        POLLING_EXTRA_PER_SLEEP * 1000
    } else {
        0
    };
    // A few allocations of the run itself are not a cost per sleep.
    assert!(
        ours_per_sleep <= compio_per_sleep + polling_extra + 8,
        "1000 sleeps allocated {ours_per_sleep} times on the {driver:?} driver, compio's own \
         {compio_per_sleep}",
    );
}

// A handler's sleeps are awaited in a spawned task, which arms the next sleep in the same turn:
// there a sleep costs what compio's own timer costs on every driver, the polling one included.
#[test]
fn a_sleep_in_turn_in_a_spawned_task_costs_what_one_timer_of_compio_costs() {
    let runtime = test_runtime().expect("a compio runtime");
    let (ours_per_sleep, compio_per_sleep) = a_thousand_sleeps(&runtime, Awaited::InSpawnedTask);
    let driver = runtime.driver_type();
    // A few allocations of the run itself are not a cost per sleep.
    assert!(
        ours_per_sleep <= compio_per_sleep + 8,
        "1000 sleeps in a spawned task allocated {ours_per_sleep} times on the {driver:?} driver, \
         compio's own {compio_per_sleep}",
    );
}

#[test]
fn a_dropped_sleep_leaves_its_room_for_the_next() {
    test_runtime().expect("a compio runtime").block_on(async {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut sleeps: Vec<Sleep> = Vec::with_capacity(10_000);
        let allocated = allocations_after_warm_up(|| {
            sleeps.extend(iter::repeat_with(|| CompioClock.sleep_until(deadline)).take(10_000));
            for sleep in &mut sleeps {
                assert!(poll_once(sleep).is_pending());
            }
            sleeps.clear();
        });
        assert_eq!(allocated, 0, "dropped sleeps kept growing the wheel");
    });
}

#[test]
fn a_thread_that_makes_a_second_runtime_gets_a_working_timer_again() {
    for _ in 0..3 {
        let runtime = test_runtime().expect("a compio runtime");
        let started = Instant::now();
        runtime.block_on(async {
            // A cancelled timer leaves a free slot behind, which the next runtime must not inherit.
            let mut dropped = time::sleep(Duration::from_secs(60));
            assert!(poll_once(&mut dropped).is_pending());
            drop(dropped);
            time::sleep(DELAY).await;
        });
        assert!(started.elapsed() >= DELAY, "the sleep ended early");
    }
}

#[test]
fn a_sleep_of_an_ended_runtime_does_not_cancel_the_timer_of_the_next() {
    let mut old = time::sleep(Duration::from_secs(60));
    test_runtime()
        .expect("a compio runtime")
        .block_on(async { assert!(poll_once(&mut old).is_pending()) });
    test_runtime().expect("a compio runtime").block_on(async {
        let mut fresh = time::sleep(DELAY);
        assert!(poll_once(&mut fresh).is_pending());
        // The old sleep gave up its place on the ended runtime; the new timer is not its place.
        drop(old);
        let started = Instant::now();
        fresh.await;
        assert!(started.elapsed() >= DELAY, "the sleep ended early");
    });
}

#[test]
fn a_sleep_polled_after_its_core_is_gone_goes_on_where_it_is_polled() {
    let (armed, from_core) = mpsc::channel::<Sleep>();
    let started = Instant::now();
    thread::spawn(move || {
        test_runtime()
            .expect("a compio runtime")
            .block_on(async move {
                let mut sleep = time::sleep(DELAY);
                assert!(poll_once(&mut sleep).is_pending());
                armed.send(sleep).expect("the test waits");
            });
    })
    .join()
    .expect("the core ended");
    let sleep = from_core.recv().expect("the sleep was handed over");
    test_runtime().expect("a compio runtime").block_on(sleep);
    assert!(started.elapsed() >= DELAY, "the sleep ended early");
}

#[test]
fn a_sleep_of_an_ended_runtime_stays_pending_once_the_next_runtime_has_slept() {
    let mut old = time::sleep(Duration::from_secs(60));
    test_runtime()
        .expect("a compio runtime")
        .block_on(async { assert!(poll_once(&mut old).is_pending()) });
    test_runtime().expect("a compio runtime").block_on(async {
        // The new runtime takes the thread's wheel before the old sleep is polled again.
        time::sleep(Duration::from_millis(1)).await;
        assert!(
            poll_once(&mut old).is_pending(),
            "the old sleep ended early"
        );
    });
}

#[test]
fn a_sleep_waiting_on_another_core_goes_on_when_the_core_that_armed_it_ends() {
    let (to_other, from_first) = mpsc::channel::<Sleep>();
    let (armed, wait_armed) = mpsc::channel::<()>();
    let (waiting, wait_waiting) = mpsc::channel::<()>();
    let (done, wait_done) = mpsc::channel::<()>();

    let first = thread::spawn(move || {
        test_runtime()
            .expect("a compio runtime")
            .block_on(async move {
                let mut sleep = time::sleep(DELAY);
                assert!(poll_once(&mut sleep).is_pending());
                to_other.send(sleep).expect("the other core is up");
                armed.send(()).expect("the test waits");
                // The runtime ends once the other core waits on the sleep.
                while wait_waiting.try_recv().is_err() {
                    time::sleep(Duration::from_millis(1)).await;
                }
            });
    });
    thread::spawn(move || {
        wait_armed.recv().expect("the first core armed its sleep");
        let mut sleep = from_first.recv().expect("the sleep was handed over");
        let mut announced = false;
        test_runtime()
            .expect("a compio runtime")
            .block_on(poll_fn(move |cx| {
                let poll = Pin::new(&mut sleep).poll(cx);
                if !announced {
                    announced = true;
                    waiting.send(()).expect("the first core waits");
                }
                poll
            }));
        done.send(()).expect("the test waits");
    });
    first.join().expect("the first core ended");
    assert!(
        wait_done.recv_timeout(Duration::from_secs(5)).is_ok(),
        "the sleep never woke after the core that armed it ended",
    );
}
