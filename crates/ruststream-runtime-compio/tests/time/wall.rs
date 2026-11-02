//! The sleep on the wall clock: `Send` however far it has got, and no allocation per timer.

use std::iter;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use allocation_counter::measure;
use compio::runtime::Runtime;
use compio::time::sleep as compio_sleep;
use futures::task::noop_waker_ref;
use ruststream_runtime::Clock;
use ruststream_runtime_compio::CompioClock;
use ruststream_runtime_compio::time::{self, Sleep};

const DELAY: Duration = Duration::from_millis(30);

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
        Runtime::new().expect("a compio runtime").block_on(sleep);
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
        Runtime::new()
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
    Runtime::new().expect("a compio runtime").block_on(sleep);
    assert!(started.elapsed() >= DELAY, "the sleep ended early");
    done.send(()).expect("the first core waits");
    first.join().expect("the first core ended");
}

#[test]
fn a_sleep_outlives_the_runtime_that_armed_it() {
    let mut sleep = time::sleep(DELAY);
    let started = Instant::now();
    {
        let runtime = Runtime::new().expect("a compio runtime");
        runtime.block_on(async {
            assert!(poll_once(&mut sleep).is_pending());
        });
    }
    Runtime::new().expect("a compio runtime").block_on(sleep);
    assert!(started.elapsed() >= DELAY, "the sleep ended early");
}

#[test]
fn a_dropped_sleep_does_not_hold_back_the_next() {
    Runtime::new().expect("a compio runtime").block_on(async {
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
    Runtime::new().expect("a compio runtime").block_on(async {
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

#[test]
fn a_sleep_in_turn_costs_what_one_timer_of_compio_costs() {
    let runtime = Runtime::new().expect("a compio runtime");
    // Allocations of `count` sleeps in turn, one by one, as `sleep` makes them.
    let cost = |count: u32, sleep: fn(Duration) -> Pin<Box<dyn Future<Output = ()>>>| {
        measure(|| {
            runtime.block_on(async {
                for _ in 0..count {
                    sleep(Duration::from_micros(20)).await;
                }
            });
        })
        .count_total
    };
    // The box is the probe's own, so it is subtracted by running both sides through it.
    let ours: fn(Duration) -> Pin<Box<dyn Future<Output = ()>>> =
        |delay| Box::pin(time::sleep(delay));
    let compio: fn(Duration) -> Pin<Box<dyn Future<Output = ()>>> =
        |delay| Box::pin(compio_sleep(delay));
    cost(1, ours);
    cost(1, compio);
    let ours_per_sleep = cost(1010, ours) - cost(10, ours);
    let compio_per_sleep = cost(1010, compio) - cost(10, compio);
    // A few allocations of the run itself are not a cost per sleep.
    assert!(
        ours_per_sleep <= compio_per_sleep + 8,
        "1000 sleeps allocated {ours_per_sleep} times, compio's own {compio_per_sleep}",
    );
}

#[test]
fn a_dropped_sleep_leaves_its_room_for_the_next() {
    Runtime::new().expect("a compio runtime").block_on(async {
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
