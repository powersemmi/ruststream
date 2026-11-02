//! The replies of a batch handler on the build without `alloc`: one per element, in the order of
//! the batch, and a reply the bus refuses reaches the `.error` continuation with the ones behind
//! it, all under an allocator that fails the framework's own allocations.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use core::str::from_utf8;
use std::cell::Cell;
use std::error::Error;
use std::iter::once;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, Outcomes, SubscriberSettings,
};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use serde::{Deserialize, Serialize};

/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;

/// The board's bus: each subscription holds four messages, waiting or in hand.
type Bus = LocalMemoryBroker<4>;

/// This test's bus.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// The stack a batch scenario with replies is held to: the build without optimisation lays every
/// frame of the settling out in full.
const STACK: usize = 8 * 1024 * 1024;

/// An ask, decoded by the codec; `0` holds the handler for a second.
#[derive(Deserialize)]
struct Ask {
    id: u8,
}

/// The answer to one ask.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Verdict {
    id: u8,
}

/// An answer as the test reads it: its postcard bytes.
#[derive(Deserialized)]
struct Answer<'a>(&'a [u8]);

/// A message the test publishes, to the destination it names: postcard bytes, so `[id]` is an ask
/// or a verdict of that id.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The id of the ask that holds the handler.
const GATE: u8 = 0;

/// Holds the handler for a second of the mock clock on the gate, so the asks behind it make one
/// batch.
async fn hold_on_gate(asks: &[Ask]) {
    if asks.iter().any(|ask| ask.id == GATE) {
        embassy_time::Timer::after(embassy_time::Duration::from_secs(1)).await;
    }
}

/// Answers every ask with its id.
#[subscriber(LocalMemorySource::new("asks"), reply)]
async fn on_asks(asks: &[Ask]) -> Outcomes<Verdict, 3> {
    hold_on_gate(asks).await;
    asks.iter().map(|ask| Verdict { id: ask.id }).collect()
}

/// Counts what a continuation was told.
#[derive(Clone, Default)]
struct Lost(Rc<Cell<u32>>);

/// Answers every ask with its id, and tells the `.error` continuation the ids it lost, as the
/// digits of one number in the order it read them.
#[subscriber(LocalMemorySource::new("lossy"), reply)]
async fn on_lossy(asks: &[Ask], ctx: &mut Context<'_, (), Lost>) -> Outcomes<Verdict, 3> {
    hold_on_gate(asks).await;
    let lost = ctx.state().clone();
    if let Ok(after) = ctx.after(HandlerOutcome::ack()) {
        after.error(async move |failures: &Failures| {
            for failed in failures {
                let id = failed
                    .message()
                    .payload()
                    .and_then(|payload| payload.first().copied())
                    .map_or(0, u32::from);
                lost.0.set(lost.0.get() * 10 + id);
            }
        });
    }
    asks.iter().map(|ask| Verdict { id: ask.id }).collect()
}

/// Three hundred bytes, past the room a lane encodes a reply into by default.
const LONG: &str = match from_utf8(&[b'x'; 300]) {
    Ok(long) => long,
    Err(_) => panic!("ascii"),
};

/// A report, which does not fit the default room once the codec has written it.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "long-reports")]
struct Report {
    text: &'static str,
}

/// Answers every ask with the long report, and counts the replies it loses.
#[subscriber(LocalMemorySource::new("reports"), reply)]
async fn on_reports(asks: &[Ask], ctx: &mut Context<'_, (), Lost>) -> Outcomes<Report, 3> {
    hold_on_gate(asks).await;
    let lost = ctx.state().clone();
    if let Ok(after) = ctx.after(HandlerOutcome::ack()) {
        after.error(async move |failures: &Failures| {
            lost.0
                .set(lost.0.get() + u32::try_from(failures.len()).unwrap_or(u32::MAX));
        });
    }
    asks.iter().map(|_| Report { text: LONG }).collect()
}

/// Takes the answers.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers(answer: &Answer<'_>) -> HandlerOutcome {
    let _ = answer.0;
    HandlerOutcome::ack()
}

/// Holds its answer for an hour of the mock clock, so the answers subscription stays full.
#[subscriber(LocalMemorySource::new("answers"))]
async fn on_answers_stuck(answer: &Answer<'_>) -> HandlerOutcome {
    let _ = answer.0;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

fn reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_answers)
            .with_codec(PostcardCodec)
            .include(on_asks.batch::<3>())
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
    })
}

fn lossy_app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.include(on_answers_stuck)
                .with_codec(PostcardCodec)
                .include(on_lossy.batch::<3>())
                .out_reply(LocalMemoryPublish)
                .codec(PostcardCodec)
        })
}

/// A bus whose messages carry up to 512 bytes, so a reply past the default encode room still fits
/// the bus.
type Wide = LocalMemoryBroker<4, 512>;

fn reports_app(bus: Wide) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.with_codec(PostcardCodec)
                .include(on_reports.batch::<3>())
                .out_reply(LocalMemoryPublish)
                .codec(PostcardCodec)
        })
}

fn roomy_reports_app(bus: Wide) -> EmbeddedApp<impl Mounts<State = Lost>> {
    EmbeddedApp::new()
        .with_state(Lost::default())
        .with_broker(bus, |b| {
            b.with_codec(PostcardCodec)
                .include(on_reports.batch::<3>())
                .out_reply(LocalMemoryPublish)
                .codec(PostcardCodec)
                .encode_capacity::<512>()
        })
}

/// Publishes the gate to `to`, then `asks` behind it, and lets the gate open: the asks arrive as
/// one batch.
async fn publish_batch(
    tb: &TestApp,
    to: &str,
    asks: [&'static [u8]; 3],
) -> Result<(), Box<dyn Error>> {
    for ask in once(&[GATE][..]).chain(asks) {
        tb.broker::<Bus>()
            .message(&Sent(ask))
            .to(to)
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

/// Publishes the gate to `reports`, then `asks` behind it, on the wide bus.
async fn publish_wide_batch(tb: &TestApp, asks: [&'static [u8]; 3]) -> Result<(), Box<dyn Error>> {
    for ask in once(&[GATE][..]).chain(asks) {
        tb.broker::<Wide>()
            .message(&Sent(ask))
            .to("reports")
            .publish()
            .await?;
    }
    tb.advance(Duration::from_secs(1)).await?;
    Ok(())
}

/// Runs a scenario on a thread of the stack [`STACK`].
fn on_big_stack(
    scenario: impl AsyncFnOnce() -> Result<(), Box<dyn Error>> + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    thread::Builder::new()
        .stack_size(STACK)
        .spawn(move || block_on(scenario()).map_err(|err| err.to_string()))?
        .join()
        .map_err(|_| "the batch scenario panicked")??;
    Ok(())
}

#[test]
fn a_batch_answers_every_element_once_in_the_order_of_the_batch_without_allocation()
-> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let tb = TestApp::start(reply_app(Bus::new(&BUS))).await?;
        publish_batch(&tb, "asks", [&[1], &[2], &[3]]).await?;

        let bus = tb.broker::<Bus>();
        let answers = bus.subscriber("answers").received_raw();
        assert_eq!(
            answers.iter().map(|one| one.to_vec()).collect::<Vec<_>>(),
            [vec![GATE], vec![1], vec![2], vec![3]],
            "the gate answers first, then the batch, element by element",
        );
        bus.subscriber("asks").assert_batch_sizes(&[1, 3]);
        tb.shutdown().await?;
        Ok(())
    })
}

/// This test's bus for the scenario where the bus refuses replies.
static LOSSY_BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

#[test]
fn replies_the_bus_refuses_reach_the_error_continuation_without_allocation()
-> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(lossy_app(Bus::new(&LOSSY_BUS)), move |_| shared).await?;
        // The stuck consumer holds the first of these, so the answers subscription has room for
        // two of the next four replies: the gate's and the batch's first.
        for body in [&[7][..], &[8]] {
            tb.broker::<Bus>()
                .message(&Sent(body))
                .to("answers")
                .publish()
                .await?;
        }
        publish_batch(&tb, "lossy", [&[1], &[2], &[3]]).await?;

        assert_eq!(
            lost.0.get(),
            23,
            "the reply at index 1 and the one behind it, in the order of the batch"
        );
        let bus = tb.broker::<Bus>();
        let outcomes = bus
            .subscriber("lossy")
            .assert_batch_sizes(&[1, 3])
            .outcomes();
        assert!(
            outcomes.iter().all(|outcome| *outcome == Outcome::Ack),
            "every element is acknowledged, whether or not its reply went out: {outcomes:?}",
        );
        tb.shutdown().await?;
        Ok(())
    })
}

/// This test's bus for the scenario of the default room.
static SHORT_BUS: LocalMemoryBus<4, 512> = LocalMemoryBus::new();

#[test]
fn replies_past_the_default_encode_room_are_refused_and_told_to_the_error_continuation()
-> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb = TestApp::with_state(reports_app(Wide::new(&SHORT_BUS)), move |_| shared).await?;
        publish_wide_batch(&tb, [&[1], &[2], &[3]]).await?;

        assert_eq!(
            lost.0.get(),
            4,
            "the gate's reply and the three of the batch did not fit"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

/// This test's bus for the scenario of the named room.
static ROOMY_BUS: LocalMemoryBus<4, 512> = LocalMemoryBus::new();

#[test]
fn a_batch_mount_with_encode_capacity_encodes_replies_past_the_default_room()
-> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let lost = Lost::default();
        let shared = lost.clone();
        let tb =
            TestApp::with_state(roomy_reports_app(Wide::new(&ROOMY_BUS)), move |_| shared).await?;
        publish_wide_batch(&tb, [&[1], &[2], &[3]]).await?;

        assert_eq!(lost.0.get(), 0, "every reply fit");
        tb.shutdown().await?;
        Ok(())
    })
}
