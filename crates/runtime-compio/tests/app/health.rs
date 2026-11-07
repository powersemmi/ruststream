//! The health probe of a running service: its state follows the lifecycle, it counts the
//! subscriptions that consume, and a stream that ends or a handler that fails flips it.
//!
//! The subject is the probe handle the started app hands out, which the harness does not expose,
//! so these tests keep `start` and its `RunningApp`.

use std::convert::Infallible;
use std::future::{Ready, pending, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use compio::runtime::spawn;
use ruststream_memory::local::LocalMemoryBroker;
use ruststream_runtime::{AppInfo, HandlerOutcome, Workers, subscriber};
use ruststream_runtime_compio::{
    Cores, HealthProbe, HealthState, RustStream, RustStreamError, SubscriptionEnd, time, yield_now,
};

use super::support::{Log, On, on_runtime, wire};

/// When the signal that ends a healthy run arrives.
const SIGNAL_AFTER: Duration = Duration::from_secs(3);

fn accept(_position: usize) -> Ready<Result<(), HandlerOutcome>> {
    ready(Ok(()))
}

/// Waits until the probe reports `active` consuming subscriptions.
async fn until_active(health: &mut HealthProbe, active: usize) {
    while health.subscriptions().active != active {
        health.changed().await;
    }
}

#[test]
fn the_probe_follows_the_lifecycle_from_running_to_stopped() {
    let log = Log::default();
    let (source, _line) = wire("orders", &log);
    let seen: Arc<Mutex<Option<HealthProbe>>> = Arc::default();
    let during = Arc::new(Mutex::new(None));
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_shutdown({
            let (seen, during) = (Arc::clone(&seen), Arc::clone(&during));
            async move |_state: Arc<()>| {
                *during.lock().expect("slot") =
                    seen.lock().expect("slot").as_ref().map(HealthProbe::state);
                Ok::<_, Infallible>(())
            }
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(source, On(accept)).build())
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let health = running.health();
        *seen.lock().expect("slot") = Some(running.health());
        assert_eq!(health.state(), HealthState::Running);
        assert!(health.is_running());

        running.shutdown().await.expect("a clean shutdown");
        assert_eq!(health.state(), HealthState::Stopped);
        assert!(!health.is_running());
    });

    assert_eq!(
        *during.lock().expect("slot"),
        Some(HealthState::ShuttingDown)
    );
}

#[test]
fn the_probe_counts_each_mount_once_whatever_its_workers() {
    let log = Log::default();
    let (orders, _orders) = wire("orders", &log);
    let (audit, _audit) = wire("audit", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.include(subscriber(orders, On(accept)).build())
                .include(subscriber(audit, On(accept)).build())
                .workers(Workers::pool(NonZeroUsize::new(3).expect("three")))
        },
    );

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let health = running.health();
        let count = health.subscriptions();
        assert_eq!((count.active, count.started), (2, 2));

        running.shutdown().await.expect("a clean shutdown");
        let count = health.subscriptions();
        assert_eq!(
            (count.active, count.started),
            (0, 2),
            "a stopped service consumes nothing, and still reports what it started"
        );
    });
}

#[test]
fn a_probe_parked_in_changed_wakes_on_the_transition() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .register_broker(LocalMemoryBroker::<8>::new());

    let woken = on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let mut health = running.health();
        let parked = spawn(async move { health.changed().await });
        // The probe parks before the shutdown moves the state.
        yield_now().await;
        running.shutdown().await.expect("a clean shutdown");
        parked.await.expect("the probe task ends")
    });

    assert_ne!(woken, HealthState::Running);
}

#[test]
fn a_stream_end_leaves_the_count_and_the_service_running_by_default() {
    let log = Log::default();
    let (orders, mut ended) = wire("orders", &log);
    let (audit, _audit) = wire("audit", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.include(subscriber(orders, On(accept)).build())
                .include(subscriber(audit, On(accept)).build())
        },
    );

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let mut health = running.health();
        ended.end();

        until_active(&mut health, 1).await;
        assert_eq!(health.subscriptions().started, 2);
        assert_eq!(
            health.state(),
            HealthState::Running,
            "one subscription ending does not end the service"
        );
        running
            .shutdown()
            .await
            .expect("a stream end under the default setting is not a failure");
    });
}

#[test]
fn a_stream_end_under_fail_fast_stops_every_core_and_run_returns_the_error() {
    let log = Log::default();
    let (orders, mut line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_subscription_end(SubscriptionEnd::FailFast)
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(orders, On(accept)).build())
        });
    line.end();

    let cores = Cores::new(NonZeroUsize::new(2).expect("two"))
        .unpinned()
        .without_signals();
    // Nothing but the stream end can stop it: the caller's own signal never comes.
    let error = app
        .run_until(cores, pending())
        .expect_err("a stream end under fail-fast must surface from run");

    match error {
        RustStreamError::Dispatch(reason) => assert!(
            reason.contains("orders"),
            "the error must name the subscription, got: {reason}"
        ),
        other => panic!("expected a dispatch failure, got {other:?}"),
    }
}

#[test]
fn a_stream_end_under_fail_fast_flips_the_probe() {
    let log = Log::default();
    let (orders, mut line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_subscription_end(SubscriptionEnd::FailFast)
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(orders, On(accept)).build())
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let mut health = running.health();
        line.end();
        running.stopping().await;

        // The watcher flips the probe right after the signal; the wait covers the hop.
        let reason = loop {
            if let HealthState::Failed { reason } = health.state() {
                break reason;
            }
            health.changed().await;
        };
        assert!(reason.contains("orders"), "got: {reason}");

        let error = running
            .shutdown()
            .await
            .expect_err("the teardown surfaces the failure");
        assert!(
            matches!(error, RustStreamError::Dispatch(_)),
            "got {error:?}"
        );
        assert_eq!(health.state(), HealthState::Failed { reason });
    });
}

#[test]
fn a_panicking_handler_fails_the_run_fast() {
    let log = Log::default();
    let (orders, line) = wire("orders", &log);
    let poster = line.poster();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .after_startup(async move |_state: Arc<()>| {
            poster.post(1);
            Ok::<_, Infallible>(())
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(orders, On(async |_| panic!("the handler broke"))).build())
        });

    let cores = Cores::new(NonZeroUsize::MIN).without_signals();
    let error = app
        .run_until(cores, pending())
        .expect_err("a handler panic fails fast by default");

    match error {
        RustStreamError::Dispatch(reason) => assert!(reason.contains("orders"), "got: {reason}"),
        other => panic!("expected a dispatch failure, got {other:?}"),
    }
}

#[test]
fn a_subscription_that_panics_outside_its_handler_fails_the_run_fast() {
    let log = Log::default();
    let (orders, _line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(orders.exploding(), On(accept)).build())
        });

    let cores = Cores::new(NonZeroUsize::MIN).without_signals();
    // The signal that ends a healthy run comes late: a failed service must not wait for it.
    let started = Instant::now();
    let error = app
        .run_until(cores, time::sleep(SIGNAL_AFTER))
        .expect_err("a panic in the subscription's loop fails the service");

    assert!(
        started.elapsed() < SIGNAL_AFTER,
        "the service ran on after its subscription died"
    );
    match error {
        RustStreamError::Dispatch(reason) => {
            assert_eq!(reason, "orders: its dispatch loop panicked");
        }
        other => panic!("expected the dispatch failure, got {other:?}"),
    }
}

/// The probe and `shutdown` name the subscription the same way, whatever the panic carried.
#[test]
fn a_subscription_that_panics_outside_its_handler_flips_the_probe_while_it_runs() {
    let log = Log::default();
    let (orders, _line) = wire("orders", &log);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(subscriber(orders.exploding_without_a_word(), On(accept)).build())
        });

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let mut health = running.health();
        // Nothing here stops the service: the panic does.
        running.stopping().await;

        // The watcher flips the probe right after the signal; the wait covers the hop.
        let reason = loop {
            if let HealthState::Failed { reason } = health.state() {
                break reason;
            }
            health.changed().await;
        };
        assert_eq!(reason, "orders: its dispatch loop panicked");

        let error = running
            .shutdown()
            .await
            .expect_err("the teardown surfaces the failure");
        match error {
            RustStreamError::Dispatch(failure) => assert_eq!(failure, reason),
            other => panic!("expected the dispatch failure, got {other:?}"),
        }
    });
}
