//! A destination hook runs on the test's own publish, through the compio `TestApp`.

// The app and its handlers are not driven by a multi-thread runtime: the harness runs them on the
// test's own compio runtime.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
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
