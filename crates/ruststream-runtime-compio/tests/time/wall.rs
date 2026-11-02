//! The sleep on the wall clock: `Send` however far it has got, and no allocation per timer.

use std::iter;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use allocation_counter::measure;
use compio::runtime::Runtime;
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

#[test]
fn timers_armed_together_cost_less_than_an_allocation_each() {
    const COUNT: usize = 1000;
    Runtime::new().expect("a compio runtime").block_on(async {
        // The first sleep starts what the core keeps for timers; the measure is the steady state.
        time::sleep(Duration::from_millis(1)).await;
        let deadline = Instant::now() + DELAY;
        let mut sleeps: Vec<Sleep> = iter::repeat_with(|| CompioClock.sleep_until(deadline))
            .take(COUNT)
            .collect();
        let costs = measure(|| {
            for sleep in &mut sleeps {
                assert!(poll_once(sleep).is_pending());
            }
        });
        assert!(
            costs.count_total < u64::try_from(COUNT / 4).expect("a small count"),
            "arming {COUNT} sleeps allocated {} times",
            costs.count_total,
        );
        time::sleep(DELAY).await;
        for sleep in &mut sleeps {
            assert!(poll_once(sleep).is_ready());
        }
    });
}
