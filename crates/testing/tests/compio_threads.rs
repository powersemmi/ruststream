//! A compio service whose subscription asks for dedicated threads is tested unchanged: the
//! harness runs its workers on the test's own runtime.

// The app and its handlers are not driven by a multi-thread runtime: the harness runs them on the
// test's own compio runtime.
#![allow(clippy::future_not_send)]

use std::error::Error;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// A frame, as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A frame the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "frames")]
struct Sent(&'static [u8]);

/// Acknowledges every frame on two threads of its own.
#[subscriber(LocalMemorySource::new("frames"), threads(2))]
async fn on_frame(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("frames", "0.1.0"))
        .with_broker(Bus::new(), |b| b.include(on_frame))
}

#[test]
fn a_threads_subscription_is_handled_and_acknowledged_under_the_harness()
-> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;

        tb.broker::<Bus>()
            .subscriber("frames")
            .assert_called_once()
            .with_raw(b"21")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

/// Acknowledges every tile on two threads of its own, a key to one thread.
#[subscriber(LocalMemorySource::new("tiles"), threads(2, by_key))]
async fn on_tile(tile: &Frame<'_>) -> HandlerOutcome {
    let _ = tile.0;
    HandlerOutcome::ack()
}

/// A tile the test publishes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "tiles")]
struct SentTile(&'static [u8]);

#[test]
fn a_keyed_threads_subscription_is_handled_and_acknowledged_under_the_harness()
-> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let app = RustStream::new(AppInfo::new("tiles", "0.1.0"))
            .with_broker(Bus::new(), |b| b.include(on_tile));
        let tb = TestApp::start(app).await?;
        tb.broker::<Bus>()
            .message(&SentTile(b"7"))
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("tiles")
            .assert_called_once()
            .with_raw(b"7")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}
