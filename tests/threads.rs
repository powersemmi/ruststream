//! Research (#417): a delivery handed to a dedicated thread (`threads(n)`) is handled on that
//! thread and only there, from the handler's first poll to its end, unless the handler sends work
//! to the app's runtime through [`MainRuntime`] on purpose.
//!
//! The subject is thread placement, which the test harness does not reproduce (under `TestApp`
//! every worker runs on the test's runtime), so these tests drive the runtime directly: a
//! multi-threaded runtime whose threads are recorded as they start, the app started on it, and
//! the deliveries published into its `MemoryBroker`.
#![cfg(all(feature = "memory", feature = "macros", feature = "json"))]

mod common;

use std::collections::HashSet;
use std::convert::Infallible;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};
use std::time::Duration;

use common::Event;
use ruststream::memory::prelude::*;
use ruststream::runtime::MainRuntime;
use tokio::runtime::Builder;
use tokio::sync::Notify;

/// Where each step of one delivery ran.
#[derive(Debug, Clone, Copy)]
struct Trace {
    start: ThreadId,
    after_timer: ThreadId,
    after_publish: ThreadId,
    spawned: ThreadId,
    on_main: ThreadId,
}

#[derive(Debug, Default)]
struct Seen {
    traces: Mutex<Vec<Trace>>,
    recorded: Notify,
}

#[subscriber("jobs")]
async fn traced(
    event: &Event,
    ctx: &mut Context<'_, (), Arc<Seen>>,
    MainRuntime(main): MainRuntime,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    let start = thread::current().id();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let after_timer = thread::current().id();
    if out.message(event).to("jobs.done").publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    let after_publish = thread::current().id();
    // A plain spawn stays where the handler runs; the explicit route goes to the app's runtime.
    let spawned = tokio::spawn(async { thread::current().id() })
        .await
        .expect("the spawned task runs");
    let on_main = main
        .spawn(async { thread::current().id() })
        .await
        .expect("the task sent to the main runtime runs");
    let seen = ctx.state();
    seen.traces.lock().unwrap().push(Trace {
        start,
        after_timer,
        after_publish,
        spawned,
        on_main,
    });
    seen.recorded.notify_one();
    HandlerOutcome::ack()
}

const DELIVERIES: u64 = 6;

/// Runs [`traced`] over `DELIVERIES` deliveries, on dedicated threads or as workers of the app's
/// runtime, and returns the traces with the ids of the app runtime's threads.
fn traces(dedicated: bool) -> (Vec<Trace>, HashSet<ThreadId>) {
    let runtime_threads = Arc::new(Mutex::new(HashSet::new()));
    let recorder = Arc::clone(&runtime_threads);
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || {
            recorder.lock().unwrap().insert(thread::current().id());
        })
        .build()
        .expect("runtime");
    let seen = Arc::new(Seen::default());
    let broker = MemoryBroker::new();
    runtime.block_on(async {
        let state = Arc::clone(&seen);
        let count = nonzero!(2);
        let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(broker.clone(), |b| {
                if dedicated {
                    b.include(traced.threads(count))
                        .out(DefaultSlot, Publish)
                        .build();
                } else {
                    b.include(traced.workers(count))
                        .out(DefaultSlot, Publish)
                        .build();
                }
            });
        let running = app.start().await.expect("startup");
        let publisher = broker.publisher();
        for id in 0..DELIVERIES {
            publisher
                .message(&Event { id })
                .to("jobs")
                .publish()
                .await
                .expect("publish");
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let recorded = seen.recorded.notified();
                if seen.traces.lock().unwrap().len() as u64 >= DELIVERIES {
                    break;
                }
                recorded.await;
            }
        })
        .await
        .expect("every delivery is handled");
        running.shutdown().await.expect("shutdown");
    });
    let traces = seen.traces.lock().unwrap().clone();
    let threads = runtime_threads.lock().unwrap().clone();
    (traces, threads)
}

/// Every step of every delivery on the thread it started on, which is none of the app runtime's;
/// what went through [`MainRuntime`] on one of the app runtime's.
fn assert_confined(traces: &[Trace], runtime: &HashSet<ThreadId>) {
    assert_eq!(traces.len() as u64, DELIVERIES);
    for trace in traces {
        assert!(
            !runtime.contains(&trace.start),
            "the handler started on the app runtime's thread: {trace:?}"
        );
        assert_eq!(
            trace.after_timer, trace.start,
            "moved across a timer: {trace:?}"
        );
        assert_eq!(
            trace.after_publish, trace.start,
            "moved across a publish: {trace:?}"
        );
        assert_eq!(
            trace.spawned, trace.start,
            "a plain spawn left the thread: {trace:?}"
        );
        assert!(
            runtime.contains(&trace.on_main),
            "the main-runtime task ran elsewhere: {trace:?}"
        );
    }
}

#[test]
fn a_delivery_on_a_dedicated_thread_is_handled_there_alone() {
    let (traces, runtime) = traces(true);
    assert_confined(&traces, &runtime);
    // Two workers, two threads of their own.
    let threads: HashSet<ThreadId> = traces.iter().map(|trace| trace.start).collect();
    assert!(threads.len() <= 2, "more threads than workers: {threads:?}");
}

/// The same handler through `workers(n)` runs on the app runtime's threads, which is what the
/// assertion above rejects: the check can fail.
#[test]
fn workers_on_the_app_runtime_fail_the_confinement_check() {
    let (traces, runtime) = traces(false);
    assert!(
        traces.iter().all(|trace| runtime.contains(&trace.start)),
        "workers(n) runs on the app runtime: {traces:?}"
    );
    let confined = catch_unwind(AssertUnwindSafe(|| assert_confined(&traces, &runtime)));
    assert!(
        confined.is_err(),
        "the confinement check passed for workers(n)"
    );
}
