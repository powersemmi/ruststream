//! A delayed retry of a device service's handler, kept in the mount's store and driven through
//! `TestApp` on embassy-time's mock clock.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::time::Duration;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// A second short of [`LATER`].
const ALMOST: Duration = Duration::from_secs(4);

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// A job the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Asks for a retry later on a first delivery, and acknowledges the copy that carries its retry
/// count.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_job(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some("1") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry_after(LATER),
    }
}

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[test]
fn a_kept_copy_is_redelivered_once_its_delay_has_passed() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"job"))
            .to("jobs")
            .publish()
            .await?;

        tb.advance(ALMOST).await?;
        tb.broker::<Bus>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(LATER));
        tb.advance(Duration::from_secs(1)).await?;
        let jobs = tb.broker::<Bus>();
        let calls = jobs.subscriber("jobs").assert_called(2);
        assert_eq!(
            calls.outcomes().last(),
            Some(&Outcome::Ack),
            "the copy carries its retry count to the handler"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_full_store_holds_the_next_failure_until_a_record_is_free() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Bus::new())).await?;
        for job in [b"one", b"two"] {
            tb.broker::<Bus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }

        // The second failure finds the one record taken: it waits, and is not delivered again.
        tb.advance(ALMOST).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(2);

        // The first copy leaves, the second failure takes its record, and the first copy is
        // handled.
        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(3);

        // The second copy waits its own delay in full.
        tb.advance(ALMOST).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(3);
        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<Bus>().subscriber("jobs").assert_called(4);
        tb.shutdown().await?;
        Ok(())
    })
}

/// A bus whose subscription holds one message, waiting or in hand.
type TightBus = LocalMemoryBroker<1>;

#[ruststream::app]
fn tight_app(bus: TightBus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_job).retry_capacity::<1>())
}

#[test]
fn a_copy_due_while_a_failure_holds_the_only_slot_is_not_lost() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(tight_app(TightBus::new())).await?;
        for job in [b"one", b"two"] {
            tb.broker::<TightBus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }

        // The second failure holds the bus's one slot while it waits for the first copy's record,
        // so the first copy cannot be published when it is due. It stays kept, and the second
        // failure gives its slot up for its own copy.
        tb.advance(LATER).await?;
        tb.broker::<TightBus>().subscriber("jobs").assert_called(2);

        // The first copy is published once the slot is free, and the second leaves after it.
        tb.advance(Duration::from_millis(100)).await?;
        tb.broker::<TightBus>().subscriber("jobs").assert_called(3);
        tb.advance(LATER).await?;
        tb.broker::<TightBus>().subscriber("jobs").assert_called(4);
        tb.shutdown().await?;
        Ok(())
    })
}
