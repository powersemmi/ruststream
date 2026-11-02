//! `Stop`: the flag a busy loop reads, the wait a parked one parks on, and the first failure.

use std::cell::Cell;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use compio::runtime::{self, Runtime};
use futures::poll;
use ruststream_runtime::{Halt, ShutdownWait};
use ruststream_runtime_compio::{Stop, SubscriptionEnd, yield_now};

fn on_runtime(test: impl Future<Output = ()>) {
    Runtime::new().expect("a compio runtime").block_on(test);
}

#[test]
fn a_failure_wakes_a_parked_wait_and_raises_the_flag() {
    on_runtime(async {
        let stop = Stop::default();
        let woke = Rc::new(Cell::new(false));
        let parked = {
            let stop = stop.clone();
            let woke = Rc::clone(&woke);
            runtime::spawn(async move {
                stop.cancelled().await;
                woke.set(true);
            })
        };
        yield_now().await;
        assert!(!woke.get());
        assert!(!stop.is_cancelled());

        stop.fail("orders", "the handler panicked");
        parked.await.expect("the parked task ends");

        assert!(woke.get());
        assert!(stop.is_cancelled());
    });
}

#[test]
fn a_wait_made_after_the_signal_is_ready_at_once() {
    on_runtime(async {
        let stop = Stop::default();
        stop.cancel();
        assert!(stop.is_cancelled());
        stop.cancelled().await;
    });
}

#[test]
fn every_clone_sees_one_signal() {
    on_runtime(async {
        let stop = Stop::default();
        let other = stop.clone();
        let mut wait = pin!(other.cancelled());
        assert_eq!(poll!(wait.as_mut()), Poll::Pending);

        stop.cancel();
        assert!(other.is_cancelled());
        assert_eq!(poll!(wait.as_mut()), Poll::Ready(()));
    });
}

#[test]
fn the_first_failure_is_the_one_reported() {
    let stop = Stop::default();
    assert_eq!(stop.failure(), None);

    stop.fail("orders.inbound", "handler panicked");
    stop.fail("other", "second");

    assert_eq!(
        stop.failure().as_deref(),
        Some("orders.inbound: handler panicked")
    );
    assert!(stop.is_cancelled());
}

#[test]
fn a_stream_ending_while_the_service_runs_follows_the_policy() {
    let logging = Stop::default();
    logging.stream_ended("orders");
    assert!(!logging.is_cancelled(), "the default reaction only logs");

    let failing = Stop::new(SubscriptionEnd::FailFast);
    failing.stream_ended("orders");
    assert!(failing.is_cancelled());
    assert_eq!(
        failing.failure().as_deref(),
        Some("orders: the subscription's stream ended")
    );
}

#[test]
fn a_stream_ending_during_shutdown_is_part_of_it() {
    let stop = Stop::new(SubscriptionEnd::FailFast);
    stop.begin();
    stop.stream_ended("orders");
    assert!(!stop.is_cancelled());
    assert_eq!(stop.failure(), None);

    let stopped = Stop::new(SubscriptionEnd::FailFast);
    stopped.cancel();
    stopped.stream_ended("orders");
    assert_eq!(stopped.failure(), None);
}
