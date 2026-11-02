//! A mount without `workers` or `threads`: the loop runs on core 0 and yields after each delivery.

use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use compio::runtime::spawn;
use futures::channel::mpsc;

use crate::support::{self, Msg, channel, collect, failure, hosted, on_cores, reporting};
use ruststream_runtime_compio::dispatch::spawn_dispatch;

#[test]
fn deliveries_are_handled_in_stream_order_on_core_zero() {
    let (handled, polled, home) = on_cores(2, async |_fabric, stop, tasks| {
        let (feed, subscriber) = channel();
        let polled = Arc::clone(&subscriber.polled);
        let (seen, mut reports) = mpsc::unbounded();
        let looped = spawn_dispatch(
            subscriber,
            reporting(seen),
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(&stop),
        );
        for position in 0..50 {
            feed.unbounded_send(Msg::numbered(position, None))
                .expect("the loop reads");
        }
        let handled = collect(&mut reports, 50).await;
        drop(looped);
        (handled, polled, thread::current().id())
    });
    let positions: Vec<usize> = handled.iter().map(|seen| seen.position).collect();
    assert_eq!(positions, (0..50).collect::<Vec<_>>());
    assert!(
        handled.iter().all(|seen| seen.thread == home),
        "a delivery left core 0"
    );
    assert!(
        polled
            .lock()
            .expect("lock")
            .iter()
            .all(|polled| *polled == home)
    );
}

#[test]
fn a_task_of_the_core_runs_between_deliveries_of_a_handler_that_never_waits() {
    // The backlog is ready at once; without the yield after a run of deliveries, the spawned task
    // would run only after all of it.
    let ran_after = on_cores(1, async |_fabric, stop, tasks| {
        let (feed, subscriber) = channel();
        for position in 0..200 {
            feed.unbounded_send(Msg::numbered(position, None))
                .expect("queued");
        }
        let (seen, mut reports) = mpsc::unbounded();
        let (ran, mut ran_at) = mpsc::unbounded();
        let counted = Arc::new(AtomicUsize::new(0));
        let handler = {
            let counted = Arc::clone(&counted);
            Arc::new(support::Reporting(move |msg: &Msg| {
                let position = msg.position();
                let _ = seen.unbounded_send(position);
                if position == 0 {
                    let (ran, counted) = (ran.clone(), Arc::clone(&counted));
                    spawn(async move {
                        let _ = ran.unbounded_send(counted.load(Ordering::SeqCst));
                    })
                    .detach();
                }
                counted.fetch_add(1, Ordering::SeqCst);
                ready(())
            }))
        };
        let looped = spawn_dispatch(
            subscriber,
            handler,
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(&stop),
        );
        let mut seen_count = 0;
        while seen_count < 200 {
            futures::StreamExt::next(&mut reports)
                .await
                .expect("reports");
            seen_count += 1;
        }
        drop(looped);
        futures::StreamExt::next(&mut ran_at)
            .await
            .expect("the task ran")
    });
    // A loop hands the core a turn after 128 deliveries in a row, the budget tokio gives a task.
    assert!(
        ran_after <= 128,
        "the spawned task ran only after {ran_after} deliveries"
    );
}
