//! The graceful shutdown: it waits for an in-flight handler and the work it leaves behind, and a
//! handler or a continuation that outlasts `shutdown_timeout` is cancelled at the cap.
//!
//! The subject is the runner's teardown, which the harness does not drive, so these tests call
//! `start`, `run_until` and `shutdown` themselves.

use std::convert::Infallible;
use std::future::{pending, ready};
use std::io;
use std::num::NonZeroUsize;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use ruststream_memory::local::LocalMemoryBroker;
use ruststream_runtime::{AppInfo, HandlerOutcome, Workers, subscriber};
use ruststream_runtime_compio::{Cores, RustStream, RustStreamError, time, yield_now};

use super::support::{Log, On, on_runtime, wire};

/// Reports to the log when it is dropped, which is how a cancelled handler is seen.
struct Dropped(Log, &'static str);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.add(self.1);
    }
}

/// Runs `run` on a thread with `count` cores and fails the test if it is still going after
/// fifteen seconds: a shutdown that hangs is a defect, not a slow test.
fn run_guarded(
    count: usize,
    run: impl FnOnce(Cores) -> Result<(), RustStreamError> + Send + 'static,
) -> Result<(), RustStreamError> {
    let (done, finished) = channel();
    let cores = Cores::new(NonZeroUsize::new(count).expect("a count"))
        .unpinned()
        .without_signals();
    thread::spawn(move || {
        let _ = done.send(run(cores));
    });
    finished
        .recv_timeout(Duration::from_secs(15))
        .expect("the run ended in time")
}

type Gate = Arc<Mutex<Option<oneshot::Receiver<()>>>>;

#[test]
fn shutdown_waits_for_an_in_flight_handler_and_its_continuation() {
    let log = Log::default();
    let (source, mut line) = wire("orders", &log);
    let (release, gate) = oneshot::channel();
    let gate: Gate = Arc::new(Mutex::new(Some(gate)));
    let (began, mut handling) = mpsc::unbounded();
    let (shutting, mut shutting_down) = mpsc::unbounded();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_shutdown(async move |_state: Arc<()>| {
            let _ = shutting.unbounded_send(());
            Ok::<_, Infallible>(())
        })
        .after_shutdown({
            let log = log.clone();
            async move |_state: Arc<()>| {
                log.add("after_shutdown");
                Ok::<_, Infallible>(())
            }
        })
        .with_broker(LocalMemoryBroker::<8>::new(), {
            let log = log.clone();
            move |b| {
                b.include(
                    subscriber(
                        source,
                        On(move |_| {
                            let (log, gate, began) =
                                (log.clone(), Arc::clone(&gate), began.clone());
                            async move {
                                log.add("handler start");
                                let _ = began.unbounded_send(());
                                let waiting =
                                    gate.lock().expect("gate").take().expect("one delivery");
                                let _ = waiting.await;
                                log.add("handler end");
                                Err(HandlerOutcome::ack().and_after(async move {
                                    time::sleep(Duration::from_millis(20)).await;
                                    log.add("continuation");
                                }))
                            }
                        }),
                    )
                    .build(),
                )
            }
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        line.post(1);
        handling.next().await.expect("the handler started");

        let shutdown = running.shutdown();
        let released = async {
            // The shutdown has begun and the handler still holds the delivery.
            shutting_down.next().await.expect("the shutdown began");
            let _ = release.send(());
        };
        let (outcome, ()) = futures::join!(shutdown, released);
        outcome.expect("a clean shutdown");
        assert_eq!(line.acked.next().await, Some(1));
    });

    let entries = log.entries();
    let at = |what: &str| {
        entries
            .iter()
            .position(|entry| entry == what)
            .unwrap_or_else(|| panic!("{what:?} is missing from {entries:?}"))
    };
    assert!(at("handler start") < at("handler end"), "{entries:?}");
    assert!(at("handler end") < at("continuation"), "{entries:?}");
    assert!(
        at("continuation") < at("after_shutdown"),
        "the shutdown waited for the continuation: {entries:?}"
    );
}

#[test]
fn a_handler_stuck_past_the_timeout_is_cancelled_at_the_cap() {
    let log = Log::default();
    let (source, line) = wire("orders", &log);
    let (began, mut handling) = mpsc::unbounded();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(100))
        .with_broker(LocalMemoryBroker::<8>::new(), {
            let log = log.clone();
            move |b| {
                b.include(
                    subscriber(
                        source,
                        On(move |_| {
                            let (log, began) = (log.clone(), began.clone());
                            async move {
                                let _stuck = Dropped(log, "handler dropped");
                                let _ = began.unbounded_send(());
                                pending::<()>().await;
                                Ok(())
                            }
                        }),
                    )
                    .build(),
                )
            }
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        line.post(1);
        handling.next().await.expect("the handler started");
        running
            .shutdown()
            .await
            .expect("the cap ends the shutdown cleanly");
    });

    assert!(
        log.entries().contains(&"handler dropped".to_owned()),
        "the stuck handler was cancelled: {:?}",
        log.entries()
    );
}

#[test]
fn a_lane_stuck_on_another_core_is_cancelled_at_the_cap_and_the_run_ends() {
    let log = Log::default();
    let (source, line) = wire("orders", &log);
    let poster = line.poster();
    let (began, mut handling) = mpsc::unbounded();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(100))
        .after_startup(async move |_state: Arc<()>| {
            poster.post(1);
            poster.post(2);
            Ok::<_, Infallible>(())
        })
        .with_broker(LocalMemoryBroker::<8>::new(), {
            let log = log.clone();
            move |b| {
                b.include(
                    subscriber(
                        source,
                        On(move |_| {
                            let (log, began) = (log.clone(), began.clone());
                            async move {
                                let _stuck = Dropped(log, "handler dropped");
                                let _ = began.unbounded_send(());
                                pending::<()>().await;
                                Ok(())
                            }
                        }),
                    )
                    .build(),
                )
                .workers(Workers::pool(NonZeroUsize::new(2).expect("two")))
            }
        });

    // Both workers hold a delivery - one runs on core 0, the other on core 1 - before the run ends.
    run_guarded(3, move |cores| {
        app.run_until(cores, async move {
            handling.next().await.expect("the first handler started");
            handling.next().await.expect("the second handler started");
        })
    })
    .expect("the cap ends the run cleanly");

    let dropped = log
        .entries()
        .iter()
        .filter(|entry| *entry == "handler dropped")
        .count();
    assert_eq!(
        dropped,
        2,
        "both stuck handlers were cancelled: {:?}",
        log.entries()
    );
}

#[test]
fn a_continuation_that_outlasts_the_timeout_is_abandoned_at_the_cap() {
    let log = Log::default();
    let (source, mut line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(100))
        .after_shutdown({
            let log = log.clone();
            async move |_state: Arc<()>| {
                log.add("after_shutdown");
                Ok::<_, Infallible>(())
            }
        })
        .with_broker(LocalMemoryBroker::<8>::new(), {
            let log = log.clone();
            move |b| {
                b.include(
                    subscriber(
                        source,
                        On(move |_| {
                            let log = log.clone();
                            ready(Err(HandlerOutcome::ack().and_after(async move {
                                let _stuck = Dropped(log, "continuation dropped");
                                pending::<()>().await;
                            })))
                        }),
                    )
                    .build(),
                )
            }
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        line.post(1);
        assert_eq!(line.acked.next().await, Some(1));
        // The continuation is spawned after the settlement; give the core its turn.
        yield_now().await;
        running
            .shutdown()
            .await
            .expect("the cap ends the shutdown cleanly");
    });

    assert!(
        log.entries().contains(&"after_shutdown".to_owned()),
        "the shutdown went on past the continuation: {:?}",
        log.entries()
    );
}

#[test]
fn a_failing_shutdown_hook_does_not_stop_the_shutdown() {
    let log = Log::default();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_shutdown(async |_state: Arc<()>| Err::<(), _>(io::Error::other("flush failed")))
        .after_shutdown({
            let log = log.clone();
            async move |_state: Arc<()>| {
                log.add("after_shutdown");
                Ok::<_, Infallible>(())
            }
        })
        .register_broker(LocalMemoryBroker::<8>::new());

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        running
            .shutdown()
            .await
            .expect("a hook that fails is logged, not returned");
    });

    assert_eq!(log.entries(), ["after_shutdown"]);
}
