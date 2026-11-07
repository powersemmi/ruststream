//! A device service's handler of one message beside a store of retries publishes through an `Out`
//! slot from the copy the store kept, driven through `TestApp` on embassy-time's mock clock.
//!
//! The app builds with no `recursion_limit` attribute, so an `Out` form beside a store that nests
//! the futures of an app deeper than the compiler's default limit fails this build.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::time::Duration;

use embassy_futures::block_on;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{
    EmbeddedApp, HandlerOutcome, MountSteps, Mounts, Out, RETRY_COUNT_HEADER,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{OutSlot, Outgoing, Publisher, Serialized, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The delay the handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

/// An ask, encoded and decoded by the codec.
#[derive(Serialize, Deserialize, Outgoing)]
struct Ask {
    id: u8,
}

/// The entry the handler writes to the audit log: the ask's id as its own bytes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "audit-log")]
struct Entry([u8; 1]);

/// The slot audit entries leave through.
#[derive(OutSlot)]
#[publishes(Entry)]
struct Audit;

/// Asks for a first delivery back later, and writes the entry from the copy the store kept.
#[subscriber(LocalMemorySource::new("asks"))]
async fn on_ask(
    ask: &Ask,
    ctx: &mut Context<'_>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        return HandlerOutcome::retry_after(LATER);
    }
    match audit.message(&Entry([ask.id])).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[ruststream::app(runtime = embassy)]
fn audit_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_ask)
            .out(Audit, LocalMemoryPublish)
            .retry_capacity::<2>()
    })
}

#[test]
fn an_out_slot_publishes_from_the_copy_a_store_kept() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(audit_app(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Ask { id: 7 })
            .to("asks")
            .publish()
            .await?;
        tb.out::<Audit>().assert_not_called();

        tb.advance(LATER).await?;
        tb.out::<Audit>().assert_called_once().with_raw(&[7]);
        tb.shutdown().await?;
        Ok(())
    })
}
