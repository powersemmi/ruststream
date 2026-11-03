//! Starting and running the service: the hooks keep their order around the subscriptions, a
//! mounted handler receives a delivery and acks it through a real run, and a startup that cannot
//! finish says why and leaves nothing open.
//!
//! The subject is the runner's lifecycle, which the harness does not drive, so these tests call
//! `start` and `run_until` themselves.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::future::ready;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::StreamExt;
use ruststream_memory::local::LocalMemoryBroker;
use ruststream_runtime::{AppInfo, Workers, subscriber};
use ruststream_runtime_compio::{Cores, HealthProbe, RustStream, RustStreamError, yield_now};

use super::support::{Log, On, on_runtime, wire};

fn cores(count: usize) -> Cores {
    Cores::new(NonZeroUsize::new(count).expect("a count"))
        .unpinned()
        .without_signals()
}

fn active(probe: &Mutex<Option<HealthProbe>>) -> Option<usize> {
    probe
        .lock()
        .expect("slot")
        .as_ref()
        .map(|probe| probe.subscriptions().active)
}

#[test]
fn the_hooks_keep_their_order_around_the_subscription() {
    let log = Log::default();
    let (source, mut line) = wire("orders", &log);
    let probe: Arc<Mutex<Option<HealthProbe>>> = Arc::default();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup({
            let log = log.clone();
            async move |_state: Arc<()>| {
                log.add("after_startup");
                Ok::<_, Infallible>(())
            }
        })
        .on_shutdown({
            let (log, probe) = (log.clone(), Arc::clone(&probe));
            async move |_state: Arc<()>| {
                log.add(format!("on_shutdown with {:?} active", active(&probe)));
                Ok::<_, Infallible>(())
            }
        })
        .after_shutdown({
            let (log, probe) = (log.clone(), Arc::clone(&probe));
            async move |_state: Arc<()>| {
                log.add(format!("after_shutdown with {:?} active", active(&probe)));
                Ok::<_, Infallible>(())
            }
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(source, On(|_| ready(Ok(())))).build())
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        *probe.lock().expect("slot") = Some(running.health());
        line.post(1);
        assert_eq!(line.acked.next().await, Some(1), "the delivery is acked");
        running.shutdown().await.expect("a clean shutdown");
    });

    assert_eq!(
        log.entries(),
        [
            "opened orders",
            "after_startup",
            "on_shutdown with Some(1) active",
            "closed orders",
            "after_shutdown with Some(0) active",
        ],
    );
}

#[test]
fn run_until_serves_a_delivery_and_returns_when_the_signal_resolves() {
    let log = Log::default();
    let (source, line) = wire("orders", &log);
    let poster = line.poster();
    let mut line = line;

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state: Arc<()>| {
            poster.post(7);
            Ok::<_, Infallible>(())
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(source, On(|_| ready(Ok(())))).build())
        });

    app.run_until(cores(1), async move {
        assert_eq!(line.acks(1).await, [7]);
    })
    .expect("the run ends cleanly");

    assert_eq!(log.entries(), ["opened orders", "closed orders"]);
}

#[test]
fn a_workers_mount_on_three_cores_delivers_through_the_fabric_and_acks() {
    let log = Log::default();
    let (source, line) = wire("orders", &log);
    let poster = line.poster();
    let mut line = line;
    let threads = Log::default();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state: Arc<()>| {
            for position in 0..4 {
                poster.post(position);
            }
            Ok::<_, Infallible>(())
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            let threads = threads.clone();
            b.include(
                subscriber(
                    source,
                    On(move |_| {
                        threads.add(format!("{:?}", thread::current().id()));
                        ready(Ok(()))
                    }),
                )
                .build(),
            )
            .workers(Workers::pool(NonZeroUsize::new(2).expect("two")))
        });

    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    app.run_until(cores(3), {
        let seen = Arc::clone(&seen);
        async move {
            let acked = line.acks(4).await;
            seen.lock().expect("seen").extend(acked);
        }
    })
    .expect("the run ends cleanly");

    assert_eq!(*seen.lock().expect("seen"), BTreeSet::from([0, 1, 2, 3]));
    let threads: BTreeSet<String> = threads.entries().into_iter().collect();
    assert_eq!(
        threads.len(),
        2,
        "the two workers run on two cores: {threads:?}"
    );
}

fn nonzero(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).expect("a count")
}

#[test]
fn threads_past_the_cores_make_run_return_the_layout_error_before_anything_starts() {
    let log = Log::default();
    let (source, _line) = wire("scans", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.include(subscriber(source, On(|_| ready(Ok(())))).build())
                .workers(Workers::threads(nonzero(2)))
        },
    );

    let error = app
        .run_until(cores(2), ready(()))
        .expect_err("two dedicated cores need three");

    assert!(matches!(error, RustStreamError::Layout(_)), "got {error:?}");
    assert!(error.to_string().contains("`scans`: 2"), "got {error}");
    assert!(log.entries().is_empty(), "nothing was opened");
}

#[test]
fn a_threads_mount_is_refused_on_the_one_core_of_start() {
    let log = Log::default();
    let (source, _line) = wire("scans", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.include(subscriber(source, On(|_| ready(Ok(())))).build())
                .workers(Workers::threads(nonzero(1)))
        },
    );

    let error = on_runtime(app.start()).expect_err("a dedicated core needs a second core");

    assert!(matches!(error, RustStreamError::Layout(_)), "got {error:?}");
}

#[test]
fn a_failing_state_producer_aborts_the_start() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async |(): ()| Err::<(), _>(io::Error::other("no database")))
        .register_broker(LocalMemoryBroker::<8>::new());

    let error = on_runtime(app.start()).expect_err("the producer failed");

    assert!(
        matches!(error, RustStreamError::Startup(_)),
        "got {error:?}"
    );
    assert!(error.to_string().contains("no database"), "got {error}");
}

#[test]
fn a_subscription_that_cannot_open_unwinds_the_start() {
    let log = Log::default();
    let (source, _line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(source.refusing(), On(|_| ready(Ok(())))).build())
        });

    on_runtime(async {
        let error = app.start().await.expect_err("the queue is refused");
        assert!(
            matches!(error, RustStreamError::Subscribe(_)),
            "got {error:?}"
        );
        assert!(
            log.bus_is_shut_down().await,
            "the connected broker is shut down"
        );
    });
}

#[test]
fn a_failing_after_startup_hook_unwinds_the_start() {
    let log = Log::default();
    let (source, _line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async |_state: Arc<()>| Err::<(), _>(io::Error::other("not ready")))
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(source, On(|_| ready(Ok(())))).build())
        });

    on_runtime(async {
        let error = app.start().await.expect_err("the hook failed");
        assert!(
            matches!(error, RustStreamError::Startup(_)),
            "got {error:?}"
        );
        assert!(
            log.bus_is_shut_down().await,
            "the connected broker is shut down"
        );
    });

    assert_eq!(
        log.entries(),
        ["opened orders", "closed orders"],
        "the loop that was started is stopped"
    );
}

#[test]
fn dropping_the_handle_detaches_the_service_instead_of_stopping_it() {
    let log = Log::default();
    let (source, mut line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(source, On(|_| ready(Ok(())))).build())
        });

    on_runtime(async {
        drop(app.start().await.expect("the service starts"));
        // Turns of the runtime in which a loop that the drop cancelled would end.
        for _ in 0..8 {
            yield_now().await;
        }
        line.post(1);
        assert_eq!(
            line.acked.next().await,
            Some(1),
            "the loop outlives the handle"
        );
    });

    assert_eq!(log.entries(), ["opened orders", "closed orders"]);
}
