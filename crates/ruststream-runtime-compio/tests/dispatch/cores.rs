//! Where the parts of a subscription run: the placement table, and core 0 receiving everything.

use std::sync::Arc;
use std::thread;

use futures::channel::mpsc;
use ruststream_runtime::Workers;
use ruststream_runtime_compio::dispatch::{Layout, Mount, spawn_dispatch_workers};

use crate::support::{Msg, channel, collect, failure, hosted, nonzero, on_cores, reporting};

#[test]
fn threads_three_on_three_cores_is_a_startup_error_naming_the_mount() {
    let refused = Layout::plan(
        3,
        &[Mount {
            name: "scans",
            workers: Workers::threads(nonzero(3)),
        }],
    )
    .expect_err("three dedicated cores leave none for core 0");
    assert!(refused.to_string().contains("`scans`: 3"), "{refused}");
}

#[test]
fn the_stream_of_a_worker_mount_is_read_on_core_zero_only() {
    let (polled, home) = on_cores(3, async |fabric, stop, tasks| {
        let workers = Workers::pool(nonzero(3));
        let layout = Layout::plan(
            3,
            &[Mount {
                name: "orders",
                workers,
            }],
        )
        .expect("fits");
        let (feed, subscriber) = channel();
        let polled = Arc::clone(&subscriber.polled);
        let (seen, mut reports) = mpsc::unbounded();
        let looped = spawn_dispatch_workers(
            subscriber,
            reporting(seen),
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(&stop),
            workers,
            &fabric,
            layout.cores(0),
        )
        .expect("the lanes start");
        for position in 0..30 {
            feed.unbounded_send(Msg::numbered(position, None))
                .expect("the loop reads");
        }
        collect(&mut reports, 30).await;
        drop(looped);
        (polled, thread::current().id())
    });
    let polled = polled.lock().expect("lock");
    assert!(!polled.is_empty());
    assert!(
        polled.iter().all(|thread| *thread == home),
        "the stream was polled off core 0"
    );
}
