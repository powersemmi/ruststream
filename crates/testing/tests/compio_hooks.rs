//! A destination hook runs on the test's own publish, through the compio `TestApp`.

// The app and its handlers are not driven by a multi-thread runtime: the harness runs them on the
// test's own compio runtime.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::time::sleep;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// A reading, as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// A reading the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "readings")]
struct Sent(&'static [u8]);

/// Acknowledges every reading.
#[subscriber(LocalMemorySource::new("readings"))]
async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("readings", "0.1.0"))
        .with_broker(Bus::new(), |b| b.include(on_reading))
}

#[test]
fn a_destination_hook_runs_for_the_tests_own_publish() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .published::<()>("readings")
            .on_publish(async move |readings| {
                readings.assert_called_once().with_raw(b"21");
                seen.fetch_add(1, Ordering::SeqCst);
            });

        tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;

        tb.settle().await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.broker::<Bus>()
            .subscriber("readings")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

/// How long a hook waits on the app's clock.
const NAP: Duration = Duration::from_secs(5);

/// Sets its flag when dropped: the hook that holds it is gone.
struct DroppedFlag(Arc<AtomicBool>);

impl Drop for DroppedFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
#[should_panic(expected = "the hook found a wrong reading")]
fn a_hook_that_fails_after_the_publish_settled_fails_the_shutdown() {
    let run = async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>()
            .published::<()>("readings")
            .on_publish(async move |_readings| {
                sleep(NAP).await;
                panic!("the hook found a wrong reading");
            });
        tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;
        tb.shutdown().await?;
        Ok::<_, Box<dyn Error>>(())
    };
    Runtime::new().unwrap().block_on(run).unwrap();
}

#[test]
fn a_hook_that_waits_on_nothing_is_dropped_before_the_shutdown_returns()
-> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        tb.broker::<Bus>()
            .published::<()>("readings")
            .on_publish(async move |_readings| {
                let _guard = DroppedFlag(Arc::clone(&flag));
                pending::<()>().await;
            });
        tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;
        assert!(!dropped.load(Ordering::SeqCst));

        tb.shutdown().await?;

        assert!(dropped.load(Ordering::SeqCst));
        Ok(())
    })
}

#[test]
fn hooks_that_ended_leave_the_harness_as_the_publishes_go_on() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>()
            .published::<()>("readings")
            .on_publish(async move |readings| {
                let _ = readings;
            });
        for _ in 0..64 {
            tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;
        }
        tb.settle().await?;
        // Each publish ran its hook to its end: none of them is still held.
        let shown = format!("{tb:?}");
        assert!(shown.contains("running: 0"), "{shown}");
        tb.shutdown().await?;
        Ok(())
    })
}
