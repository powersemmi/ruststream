//! A handler of one message publishes through an `Out` slot on the build without `alloc`: the
//! harness records the publish as its own bookkeeping, under an allocator that fails the
//! framework's own allocations.
//!
//! The app builds with no `recursion_limit` attribute, so an `Out` form that nests the futures of
//! an app deeper than the compiler's default limit fails this build.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::error::Error;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::testing::embedded::TestApp;
use ruststream::{OutSlot, Outgoing, Publisher, Serialized, subscriber};
use serde::Deserialize;

/// Fails an allocation made while the harness polls the app.
#[global_allocator]
static ALLOCATOR: AllocDisabler = AllocDisabler;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// This test's bus.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// An ask, decoded by the codec.
#[derive(Deserialize)]
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

/// A message the test publishes, to the destination it names: postcard bytes, so `[id]` is an ask.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// Writes one entry for the ask through the slot.
#[subscriber(LocalMemorySource::new("asks"))]
async fn on_ask(ask: &Ask, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    match audit.message(&Entry([ask.id])).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[ruststream::app(runtime = embassy)]
fn audit_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(on_ask)
            .out(Audit, LocalMemoryPublish)
    })
}

#[test]
fn an_out_slot_publish_is_recorded_without_allocation() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(audit_app(Bus::new(&BUS))).await?;
        tb.broker::<Bus>()
            .message(&Sent(&[5]))
            .to("asks")
            .publish()
            .await?;

        tb.out::<Audit>().assert_called_once().with_raw(&[5]);
        tb.shutdown().await?;
        Ok(())
    })
}
