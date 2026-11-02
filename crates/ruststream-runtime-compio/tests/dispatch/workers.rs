//! `workers(n)` and `threads(n)`: deliveries handed from core 0 to lanes on other cores.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use compio::runtime::spawn;
use event_listener::Event;
use futures::StreamExt;
use futures::channel::mpsc;
use ruststream_runtime::Workers;
use ruststream_runtime_compio::dispatch::{Layout, Mount, spawn_dispatch_workers};

use crate::support::{
    Msg, Reporting, channel, collect, failure, hosted, nonzero, on_cores, reporting,
};

/// A barrier a handler awaits without blocking its core: it opens when `parties` handlers are in.
struct Gate {
    parties: usize,
    arrived: AtomicUsize,
    opened: Event,
}

impl Gate {
    fn new(parties: usize) -> Arc<Self> {
        Arc::new(Self {
            parties,
            arrived: AtomicUsize::new(0),
            opened: Event::new(),
        })
    }

    async fn pass(&self) {
        self.arrived.fetch_add(1, Ordering::SeqCst);
        self.opened.notify(usize::MAX);
        while self.arrived.load(Ordering::SeqCst) < self.parties {
            let listener = self.opened.listen();
            if self.arrived.load(Ordering::SeqCst) >= self.parties {
                break;
            }
            listener.await;
        }
    }
}

#[test]
fn workers_three_have_three_deliveries_in_flight_at_once() {
    // Every handler waits for the other two: the test passes only if three deliveries are being
    // handled at the same time.
    let handled = on_cores(3, async |fabric, stop, tasks| {
        let layout = Layout::plan(
            3,
            &[Mount {
                name: "orders",
                workers: Workers::pool(nonzero(3)),
            }],
        )
        .expect("fits");
        let (feed, subscriber) = channel();
        let (seen, mut reports) = mpsc::unbounded();
        let gate = Gate::new(3);
        let handler = Arc::new(Reporting(move |msg: &Msg| {
            let position = msg.position();
            let (gate, seen) = (Arc::clone(&gate), seen.clone());
            async move {
                gate.pass().await;
                let _ = seen.unbounded_send(position);
            }
        }));
        let looped = spawn_dispatch_workers(
            subscriber,
            handler,
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(&stop),
            Workers::pool(nonzero(3)),
            &fabric,
            layout.cores(0),
        )
        .expect("the lanes start");
        for position in 0..3 {
            feed.unbounded_send(Msg::numbered(position, None))
                .expect("the loop reads");
        }
        let mut order = Vec::new();
        for _ in 0..3 {
            order.push(reports.next().await.expect("reports"));
        }
        drop(looped);
        order
    });
    let mut handled = handled;
    handled.sort_unstable();
    assert_eq!(handled, [0, 1, 2]);
}

#[test]
fn threads_two_on_three_cores_run_on_cores_one_and_two_only() {
    // The deliveries and the threads of the cores come from one run, so the ids are comparable.
    let (seen, threads) = on_cores(3, async |fabric, stop, tasks| {
        let layout = Layout::plan(
            3,
            &[Mount {
                name: "scans",
                workers: Workers::threads(nonzero(2)),
            }],
        )
        .expect("threads(2) fits on three cores");
        let (feed, subscriber) = channel();
        let (seen, mut reports) = mpsc::unbounded();
        let looped = spawn_dispatch_workers(
            subscriber,
            reporting(seen),
            stop.clone(),
            Arc::from("scans"),
            Arc::new(()),
            hosted(tasks),
            failure(&stop),
            Workers::threads(nonzero(2)),
            &fabric,
            layout.cores(0),
        )
        .expect("the lanes start");
        for position in 0..40 {
            feed.unbounded_send(Msg::numbered(position, None))
                .expect("the loop reads");
        }
        let seen = collect(&mut reports, 40).await;
        drop(looped);
        // Each core says which thread it is.
        let (here, mut found) = mpsc::unbounded();
        for core in 0..3 {
            let here = here.clone();
            fabric
                .run_on(core, "probe", move || {
                    spawn(async move {
                        let _ = here.unbounded_send((core, thread::current().id()));
                    })
                })
                .expect("the core serves");
        }
        let mut threads = vec![thread::current().id(); 3];
        for _ in 0..3 {
            let (core, thread) = found.next().await.expect("each core reports");
            threads[core] = thread;
        }
        (seen, threads)
    });
    assert!(
        seen.iter().all(|seen| seen.thread != threads[0]),
        "a threads(2) delivery ran on core 0"
    );
    for core in [1, 2] {
        assert!(
            seen.iter().any(|seen| seen.thread == threads[core]),
            "no delivery reached core {core}"
        );
    }
}
