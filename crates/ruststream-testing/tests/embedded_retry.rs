//! A delayed retry of a device service's handler, kept in the mount's store and driven through
//! `TestApp` on embassy-time's mock clock.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::sync::Mutex;
use std::time::Duration;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::testing::Outcome;
use ruststream::testing::TestApp;
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

#[ruststream::app]
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

/// How often the handler saw each job of the tight board, by its first byte.
#[derive(Default)]
struct Seen(Mutex<[u32; 3]>);

/// Fails the first delivery of a job and acks the next; job `a` fails its first two.
#[subscriber(LocalMemorySource::new("jobs"))]
async fn on_tight_job(job: &Job<'_>, ctx: &mut Context<'_, (), Seen>) -> HandlerOutcome {
    let index = usize::from(job.0[0] - b'a');
    let seen = ctx.state();
    let mut counts = seen.0.lock().expect("the handler never panics");
    counts[index] += 1;
    let failures = if index == 0 { 2 } else { 1 };
    if counts[index] <= failures {
        HandlerOutcome::retry_after(LATER)
    } else {
        HandlerOutcome::ack()
    }
}

#[ruststream::app]
fn tight_app(bus: TightBus) -> EmbeddedApp<impl Mounts<State = Seen>> {
    EmbeddedApp::new()
        .with_state(Seen::default())
        .with_broker(bus, |b| b.include(on_tight_job).retry_capacity::<1>())
}

#[test]
fn a_failure_holding_the_only_slot_goes_back_to_the_broker_when_a_copy_needs_it()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::with_state(tight_app(TightBus::new()), |_| Seen::default()).await?;
        for job in [b"a", b"b"] {
            tb.broker::<TightBus>()
                .message(&Sent(job))
                .to("jobs")
                .publish()
                .await?;
        }

        // `b` holds the bus's one slot while it waits for `a`'s record, so `a`'s copy is refused
        // when it falls due. `b` goes back to the broker and is handled again, and `a`'s copy
        // follows once the slot is free.
        tb.advance(LATER).await?;
        tb.broker::<TightBus>().subscriber("jobs").assert_called(3);
        tb.advance(Duration::from_millis(100)).await?;
        tb.broker::<TightBus>().subscriber("jobs").assert_called(4);

        // `a` fails again and takes the record; `c` fails behind it and gives the slot up the same
        // way, and `a` is handled to its end.
        tb.broker::<TightBus>()
            .message(&Sent(b"c"))
            .to("jobs")
            .publish()
            .await?;
        tb.advance(LATER).await?;
        tb.broker::<TightBus>().subscriber("jobs").assert_called(6);
        tb.advance(Duration::from_millis(100)).await?;
        let jobs = tb.broker::<TightBus>();
        let calls = jobs.subscriber("jobs").assert_called(7);
        assert_eq!(
            calls.outcomes().last(),
            Some(&Outcome::Ack),
            "the third delivery of `a` is its last"
        );
        tb.shutdown().await?;
        Ok(())
    })
}
