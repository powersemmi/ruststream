//! A batch handler of the embedded runner takes `Out` slots: it publishes through them once per
//! publish, and the harness records each under its slot. Driven through `TestApp` on
//! embassy-time's mock clock.
//!
//! The harness lets the app handle each publish before it returns, so a batch of several asks is
//! made the way a busy bus makes it: the first ask holds the handler for a second of the mock
//! clock and the rest queue behind it.

// The apps and their handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::iter::once;
use std::time::Duration;

use embassy_futures::block_on;
use embassy_time::{Duration as EmbassyDuration, Timer};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{
    EmbeddedApp, HandlerOutcome, MountSteps, Mounts, Out, SubscriberSettings,
};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages, waiting or in hand.
type Bus = LocalMemoryBroker<4>;

/// An ask, decoded by the codec; `0` holds the handler for a second.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
struct Ask {
    id: u8,
}

/// The entry the handler writes to the audit log for each ask.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "audit-log")]
struct Entry {
    id: u8,
}

/// The slot audit entries leave through.
#[derive(OutSlot)]
#[publishes(Entry)]
struct Audit;

/// The id of the ask that holds the handler.
const GATE: u8 = 0;

/// Writes one entry per ask through the slot.
#[subscriber(LocalMemorySource::new("asks"))]
async fn on_asks(asks: &[Ask], Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    if asks.iter().any(|ask| ask.id == GATE) {
        Timer::after(EmbassyDuration::from_secs(1)).await;
    }
    for ask in asks {
        if audit
            .message(&Entry { id: ask.id })
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

/// The delay the retrying handler asks for.
const LATER: Duration = Duration::from_secs(5);

/// Writes one entry per ask through the slot, and asks for the batch back later.
#[subscriber(LocalMemorySource::new("retried-asks"))]
async fn on_retried_asks(asks: &[Ask], Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    for ask in asks {
        if audit
            .message(&Entry { id: ask.id })
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::retry_after(LATER)
}

#[ruststream::app(runtime = embassy)]
fn retrying_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_retried_asks.batch(nonzero!(1)))
            .out(Audit, LocalMemoryPublish)
            .retry_capacity::<1>()
    })
}

#[ruststream::app(runtime = embassy)]
fn audit_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_asks.batch(nonzero!(3)))
            .out(Audit, LocalMemoryPublish)
    })
}

#[test]
fn a_batch_handler_publishes_through_its_slot_once_per_publish() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(audit_app(Bus::new())).await?;
        for id in once(GATE).chain([1, 2, 3]) {
            tb.broker::<Bus>()
                .message(&Ask { id })
                .to("asks")
                .publish()
                .await?;
        }
        tb.advance(Duration::from_secs(1)).await?;

        let entries = [
            Entry { id: GATE },
            Entry { id: 1 },
            Entry { id: 2 },
            Entry { id: 3 },
        ];
        assert_eq!(
            tb.out::<Audit>()
                .assert_called(entries.len())
                .decoded_as::<Entry>()
                .decoded(),
            entries,
            "the gate's entry first, then the batch's, one per ask",
        );
        let bus = tb.broker::<Bus>();
        assert_eq!(
            bus.published::<Entry>("audit-log")
                .assert_called(entries.len())
                .decoded(),
            entries,
            "each entry reached the audit log once",
        );
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

#[test]
fn a_batch_handler_with_a_slot_and_a_store_publishes_again_when_its_copy_returns()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(retrying_app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Ask { id: 7 })
            .to("retried-asks")
            .publish()
            .await?;
        tb.out::<Audit>().assert_called_once();

        tb.advance(LATER).await?;

        assert_eq!(
            tb.out::<Audit>()
                .assert_called(2)
                .decoded_as::<Entry>()
                .decoded(),
            [Entry { id: 7 }, Entry { id: 7 }],
            "the redelivered batch publishes through its slot again",
        );
        tb.shutdown().await?;
        Ok(())
    })
}
