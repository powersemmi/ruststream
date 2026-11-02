//! The compio `TestApp` settles when the app has nothing left to run: work that waits on the app's
//! clock is left for `advance`, a long reaction is followed to its end, and work that waits on
//! nothing is reported.

#![allow(clippy::future_not_send)]

use std::error::Error;
use std::future;
use std::str::from_utf8;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::compio::time::sleep;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, Out, PublishError};
use ruststream::testing::TestError;
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// How long the sleeper waits on the app's clock.
const NAP: Duration = Duration::from_secs(5);

/// The hops a chain makes after its first delivery.
const HOPS: usize = 1_000;

/// A message, as its bytes.
#[derive(Deserialized)]
struct Note<'a>(&'a [u8]);

/// A message the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(Vec<u8>);

/// The next hop of the chain, addressed to the chain itself.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "chain")]
struct Hop(Vec<u8>);

/// The slot the chain continues through.
#[derive(OutSlot)]
#[publishes(Hop)]
struct Next;

/// Waits on the app's clock before it acknowledges.
#[subscriber(LocalMemorySource::new("naps"))]
async fn nap(note: &Note<'_>) -> HandlerOutcome {
    let _ = note.0;
    sleep(NAP).await;
    HandlerOutcome::ack()
}

/// Waits on nothing that wakes it.
#[subscriber(LocalMemorySource::new("stuck"))]
async fn stuck(note: &Note<'_>) -> HandlerOutcome {
    let _ = note.0;
    future::pending::<()>().await;
    HandlerOutcome::ack()
}

/// Publishes the next hop until the count it carries reaches zero.
#[subscriber(LocalMemorySource::new("chain"))]
async fn hop(note: &Note<'_>, Out(next): Out<impl Publisher, Next>) -> HandlerOutcome {
    let left: usize = from_utf8(note.0)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(0);
    if left > 0 {
        let sent = next
            .message(&Hop((left - 1).to_string().into_bytes()))
            .publish()
            .await;
        if sent.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("quiescence", "0.1.0")).with_broker(Bus::new(), |b| {
        b.include(nap)
            .include(stuck)
            .include(hop)
            .out(Next, LocalMemoryPublish)
    })
}

#[test]
fn a_handler_sleeping_on_the_app_clock_waits_for_advance() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"z".to_vec()))
            .to("naps")
            .publish()
            .await?;
        tb.settle().await?;
        // A call is recorded once its handler has finished, and this one still sleeps.
        tb.broker::<Bus>().subscriber("naps").assert_not_called();

        tb.advance(NAP).await?;
        tb.broker::<Bus>()
            .subscriber("naps")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_long_reaction_settles_in_one_publish_beside_a_timer() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        // A timer is armed, so a harness that counts turns takes the app for settled early.
        tb.broker::<Bus>()
            .message(&Sent(b"z".to_vec()))
            .to("naps")
            .publish()
            .await?;
        tb.broker::<Bus>()
            .message(&Sent(HOPS.to_string().into_bytes()))
            .to("chain")
            .publish()
            .await?;
        tb.broker::<Bus>()
            .subscriber("chain")
            .assert_called(HOPS + 1);
        tb.advance(NAP).await?;
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_handler_that_waits_on_nothing_stalls_the_settle() -> Result<(), Box<dyn Error>> {
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        let published = tb
            .broker::<Bus>()
            .message(&Sent(b"z".to_vec()))
            .to("stuck")
            .publish()
            .await;
        assert!(
            matches!(
                published,
                Err(PublishError::Publish {
                    source: TestError::Stalled,
                    ..
                })
            ),
            "the publish returned {published:?}"
        );
        let settled = tb.settle().await;
        assert!(
            matches!(settled, Err(TestError::Stalled)),
            "settle returned {settled:?}"
        );
        Ok(())
    })
}

#[test]
fn a_call_hook_runs_before_the_publish_that_caused_the_call_returns() -> Result<(), Box<dyn Error>>
{
    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app()).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.broker::<Bus>()
            .subscriber("chain")
            .on_call(async move |calls| {
                calls
                    .assert_called(seen.fetch_add(1, Ordering::SeqCst) + 1)
                    .settled(HandlerOutcome::ack());
            });

        tb.broker::<Bus>()
            .message(&Sent(b"0".to_vec()))
            .to("chain")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.shutdown().await?;
        Ok(())
    })
}
