//! A device service's batch handler publishes through an `Out` slot once per ask, driven through
//! `TestApp` on embassy-time's mock clock.
//!
//! The app builds with no `recursion_limit` attribute, so a batch `Out` form that nests the futures
//! of an app deeper than the compiler's default limit fails this build.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::nonzero;
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out, SubscriberSettings};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{OutSlot, Outgoing, Publisher, Serialized, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// An ask, encoded and decoded by the codec.
#[derive(Serialize, Deserialize, Outgoing)]
struct Ask {
    id: u8,
}

/// The entry the handler writes to the audit log for each ask: its id as its own bytes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "audit-log")]
struct Entry([u8; 1]);

/// The slot audit entries leave through.
#[derive(OutSlot)]
#[publishes(Entry)]
struct Audit;

/// Writes one entry per ask through the slot.
#[subscriber(LocalMemorySource::new("asks"))]
async fn on_asks(asks: &[Ask], Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
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
        b.include(on_asks.batch(nonzero!(3)))
            .out(Audit, LocalMemoryPublish)
    })
}

#[test]
fn a_batch_handler_publishes_through_its_out_slot_once_per_ask() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(audit_app(Bus::new())).await?;
        for id in [1, 2, 3] {
            tb.broker::<Bus>()
                .message(&Ask { id })
                .to("asks")
                .publish()
                .await?;
        }

        let entries = tb.out::<Audit>().assert_called(3);
        assert_eq!(
            entries
                .messages()
                .iter()
                .map(|entry| entry.payload().to_vec())
                .collect::<Vec<_>>(),
            [vec![1], vec![2], vec![3]],
            "one entry per ask, in the order the asks came",
        );
        let outcomes = tb.broker::<Bus>().subscriber("asks").outcomes();
        assert!(
            outcomes.iter().all(|outcome| *outcome == Outcome::Ack),
            "every ask is acknowledged: {outcomes:?}",
        );
        tb.shutdown().await?;
        Ok(())
    })
}
