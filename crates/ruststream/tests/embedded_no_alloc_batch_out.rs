//! A batch handler on the build without `alloc` takes `Out` slots: it publishes through them once
//! per publish, and the harness records each under its slot, all under an allocator that fails the
//! framework's own allocations.
//!
//! The app builds with no `recursion_limit` attribute, so a batch `Out` form that nests the futures
//! of an app deeper than the compiler's default limit fails this build.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::error::Error;
use std::iter::once;
use std::thread;
use std::time::Duration;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out, SubscriberSettings};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{OutSlot, Outgoing, Publisher, Serialized, subscriber};
use serde::Deserialize;

/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;

/// The board's bus: each subscription holds four messages, waiting or in hand.
type Bus = LocalMemoryBroker<4>;

/// This test's bus.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// The stack a batch scenario is held to: the build without optimisation lays every frame of the
/// settling out in full.
const STACK: usize = 8 * 1024 * 1024;

/// An ask, decoded by the codec; `0` holds the handler for a second.
#[derive(Deserialize)]
struct Ask {
    id: u8,
}

/// The entry the handler writes to the audit log for each ask: the ask's id as its own bytes, which
/// the codec-free slot sends as they are.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "audit-log")]
struct Entry([u8; 1]);

/// The slot audit entries leave through.
#[derive(OutSlot)]
#[publishes(Entry)]
struct Audit;

/// A message the test publishes, to the destination it names: postcard bytes, so `[id]` is an ask.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The id of the ask that holds the handler.
const GATE: u8 = 0;

/// Writes one entry per ask through the slot.
#[subscriber(LocalMemorySource::new("asks"))]
async fn on_asks(asks: &[Ask], Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    if asks.iter().any(|ask| ask.id == GATE) {
        embassy_time::Timer::after(embassy_time::Duration::from_secs(1)).await;
    }
    for ask in asks {
        if audit.message(&Entry([ask.id])).publish().await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn audit_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(on_asks.batch::<3>())
            .out(Audit, LocalMemoryPublish)
    })
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
fn a_batch_handler_publishes_through_its_slot_once_per_publish_without_allocation()
-> Result<(), Box<dyn Error>> {
    on_big_stack(async || {
        let tb = TestApp::start(audit_app(Bus::new(&BUS))).await?;
        for ask in once(&[GATE][..]).chain([&[1][..], &[2], &[3]]) {
            tb.broker::<Bus>()
                .message(&Sent(ask))
                .to("asks")
                .publish()
                .await?;
        }
        tb.advance(Duration::from_secs(1)).await?;

        let entries = tb.out::<Audit>().assert_called(4);
        assert_eq!(
            entries
                .messages()
                .iter()
                .map(|entry| entry.payload().to_vec())
                .collect::<Vec<_>>(),
            [vec![GATE], vec![1], vec![2], vec![3]],
            "the gate's entry first, then the batch's, one per ask",
        );
        let bus = tb.broker::<Bus>();
        let outcomes = bus
            .subscriber("asks")
            .assert_batch_sizes(&[1, 3])
            .outcomes();
        assert!(
            outcomes.iter().all(|outcome| *outcome == Outcome::Ack),
            "every ask is acknowledged: {outcomes:?}",
        );
        tb.shutdown().await?;
        Ok(())
    })
}
