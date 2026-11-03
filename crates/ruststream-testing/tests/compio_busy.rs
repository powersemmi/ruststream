//! A shutdown that fails on a handler which keeps itself runnable still leaves nothing behind
//! on the compio `TestApp`: the handler is gone and `after_shutdown`, which runs once the brokers
//! are shut down, has run.

#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublisher, LocalMemorySource};
use ruststream::runtime::compio::time::sleep;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, State};
use ruststream::testing::TestError;
use ruststream::testing::compio::TestApp;
use ruststream::{
    BytesMut, Deserialized, FromRef, Outgoing, OutgoingMessage, Publisher, Serialized, subscriber,
};

type Bus = LocalMemoryBroker<4>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "spin")]
struct Spin(Vec<u8>);

/// How often the spinning handler has run.
#[derive(Clone)]
struct Ticks(Arc<AtomicU64>);

/// The app's state.
#[derive(FromRef)]
struct Busy {
    ticks: Ticks,
}

/// Gives the other tasks a turn and asks to be polled again at once.
async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Counts every turn it takes and never ends.
#[subscriber(LocalMemorySource::new("spin"))]
async fn on_spin(job: &Job<'_>, State(ticks): State<Ticks>) -> HandlerOutcome {
    let _ = job.0;
    loop {
        ticks.0.fetch_add(1, Ordering::Relaxed);
        yield_once().await;
    }
}

#[test]
fn a_failed_shutdown_stops_the_handler_and_runs_after_shutdown() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let ticks = Ticks(Arc::new(AtomicU64::new(0)));
        let after_ran = Arc::new(AtomicBool::new(false));
        let (state_ticks, hook_ran) = (ticks.clone(), Arc::clone(&after_ran));
        let app = RustStream::new(AppInfo::new("busy", "0.1.0"))
            .on_startup(async move |(): ()| Ok::<_, Infallible>(Busy { ticks: state_ticks }))
            .after_shutdown(async move |_: Arc<Busy>| {
                hook_ran.store(true, Ordering::Relaxed);
                Ok::<_, Infallible>(())
            })
            .with_broker(Bus::new(), |b| b.include(on_spin));
        let tb = TestApp::start(app).await?;
        let _ = tb
            .broker::<Bus>()
            .message(&Spin(b"1".to_vec()))
            .publish()
            .await;

        let stopped = tb.shutdown().await;

        assert!(
            matches!(stopped, Err(TestError::KeptRunning { .. })),
            "the shutdown returned {stopped:?}",
        );
        assert!(
            after_ran.load(Ordering::Relaxed),
            "after_shutdown did not run"
        );
        // A cancelled task is seen at its next turn, so it is given a few before the count is read.
        for _ in 0..10 {
            yield_once().await;
        }
        let at_return = ticks.0.load(Ordering::Relaxed);
        for _ in 0..200 {
            yield_once().await;
        }
        assert_eq!(
            ticks.0.load(Ordering::Relaxed),
            at_return,
            "the handler kept running after the shutdown returned",
        );
        Ok(())
    })
}

#[test]
fn an_after_shutdown_hook_that_sleeps_finishes_after_a_failed_shutdown()
-> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let after_ran = Arc::new(AtomicBool::new(false));
        let hook_ran = Arc::clone(&after_ran);
        let app = RustStream::new(AppInfo::new("busy-sleep", "0.1.0"))
            .on_startup(async move |(): ()| {
                Ok::<_, Infallible>(Busy {
                    ticks: Ticks(Arc::new(AtomicU64::new(0))),
                })
            })
            .after_shutdown(async move |_: Arc<Busy>| {
                sleep(Duration::from_secs(5)).await;
                hook_ran.store(true, Ordering::Relaxed);
                Ok::<_, Infallible>(())
            })
            .with_broker(Bus::new(), |b| b.include(on_spin));
        let tb = TestApp::start(app).await?;
        let _ = tb
            .broker::<Bus>()
            .message(&Spin(b"1".to_vec()))
            .publish()
            .await;

        let stopped = tb.shutdown().await;

        assert!(
            matches!(stopped, Err(TestError::KeptRunning { .. })),
            "the shutdown returned {stopped:?}",
        );
        assert!(
            after_ran.load(Ordering::Relaxed),
            "after_shutdown did not finish",
        );
        Ok(())
    })
}

/// The app's state, with the way into the bus the startup hook sets work off through.
#[derive(FromRef)]
struct Fed {
    ticks: Ticks,
    bus: Option<LocalMemoryPublisher<4>>,
}

/// Why the startup fails after it has set work running.
#[derive(Debug)]
struct NoConfig;

impl fmt::Display for NoConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no configuration")
    }
}

impl Error for NoConfig {}

#[test]
fn a_failed_startup_stops_the_handler_it_set_running_and_closes_the_broker()
-> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let ticks = Ticks(Arc::new(AtomicU64::new(0)));
        let state_ticks = ticks.clone();
        let unused = ticks.clone();
        let app = RustStream::new(AppInfo::new("busy-start", "0.1.0"))
            .on_startup(async move |(): ()| {
                Ok::<_, Infallible>(Fed {
                    ticks: unused,
                    bus: None,
                })
            })
            .after_startup(async move |fed: Arc<Fed>| {
                let job = OutgoingMessage::produced("spin", BytesMut::from(&b"1"[..]));
                if let Some(bus) = &fed.bus {
                    let _ = bus.publish(job, None).await;
                }
                // The handler is given its turns, so it is running when the startup fails.
                for _ in 0..10 {
                    yield_once().await;
                }
                Err::<(), _>(NoConfig)
            })
            .with_broker(Bus::new(), |b| b.include(on_spin));
        let kept = Arc::new(Mutex::new(None));
        let keep = Arc::clone(&kept);
        let started = TestApp::with_state(app, move |brokers| {
            let bus = brokers.broker::<Bus>().publisher();
            *keep.lock().expect("the test is the only user") = Some(bus.clone());
            Fed {
                ticks: state_ticks,
                bus: Some(bus),
            }
        })
        .await;
        assert!(
            matches!(started, Err(TestError::Startup(_))),
            "the start returned {started:?}",
        );
        for _ in 0..10 {
            yield_once().await;
        }
        let at_return = ticks.0.load(Ordering::Relaxed);
        assert!(at_return > 0, "the handler never ran");
        for _ in 0..200 {
            yield_once().await;
        }
        assert_eq!(
            ticks.0.load(Ordering::Relaxed),
            at_return,
            "the handler kept running after the start failed",
        );
        let bus = kept
            .lock()
            .expect("the test is the only user")
            .take()
            .expect("the state was built");
        let job = OutgoingMessage::produced("spin", BytesMut::from(&b"2"[..]));
        assert!(
            bus.publish(job, None).await.is_err(),
            "the broker was left open after the start failed",
        );
        Ok(())
    })
}
