//! A shutdown that fails on a handler which keeps itself runnable still leaves nothing behind
//! on the compio `TestApp`: the handler is gone and `after_shutdown`, which runs once the brokers
//! are shut down, has run.

#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::error::Error;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Poll;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, State};
use ruststream::testing::TestError;
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, FromRef, Outgoing, Serialized, subscriber};

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
