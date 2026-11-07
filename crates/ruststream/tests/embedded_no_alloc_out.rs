//! A handler of one message publishes through an `Out` slot on the build without `alloc`: the
//! harness records the publish as its own bookkeeping, under an allocator that fails the
//! framework's own allocations, and a publish the bus refuses hands its message back from the
//! slot's place.
//!
//! The app builds with no `recursion_limit` attribute, so an `Out` form that nests the futures of
//! an app deeper than the compiler's default limit fails this build.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use std::cell::Cell;
use std::error::Error;
use std::rc::Rc;

use assert_no_alloc::AllocDisabler;
use futures::executor::block_on;
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{EmbeddedApp, FailedMessage, HandlerOutcome, Mounts, Out};
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

/// The first byte of the message a refused publish handed back.
#[derive(Clone, Default)]
struct Kept(Rc<Cell<Option<u8>>>);

/// Writes one entry for the ask through the slot, and keeps the first byte of an entry the bus
/// refused.
#[subscriber(LocalMemorySource::new("pressed"))]
async fn on_pressed(
    ask: &Ask,
    ctx: &mut Context<'_, (), Kept>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    if let Err(refused) = audit.message(&Entry([ask.id])).publish().await {
        let first = refused
            .unsent()
            .and_then(FailedMessage::payload)
            .and_then(|payload| payload.first().copied());
        ctx.state().0.set(first);
    }
    HandlerOutcome::ack()
}

/// Holds the audit log full.
#[subscriber(LocalMemorySource::new("audit-log"))]
async fn on_audit_stuck(held: &Ask) -> HandlerOutcome {
    let _ = held.id;
    embassy_time::Timer::after(embassy_time::Duration::from_secs(3600)).await;
    HandlerOutcome::ack()
}

/// A bus whose subscriptions hold one message each.
type Tight = LocalMemoryBroker<1>;

/// One bus for the refused-entry scenario.
static TIGHT_BUS: LocalMemoryBus<1> = LocalMemoryBus::new();

fn pressed_app(bus: Tight) -> EmbeddedApp<impl Mounts<State = Kept>> {
    EmbeddedApp::new()
        .with_state(Kept::default())
        .with_broker(bus, |b| {
            b.with_codec(PostcardCodec)
                .include(on_audit_stuck)
                .include(on_pressed)
                .out(Audit, LocalMemoryPublish)
        })
}

#[test]
fn a_refused_slot_publish_hands_back_the_bytes_its_place_kept() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let kept = Kept::default();
        let shared = kept.clone();
        let tb = TestApp::with_state(pressed_app(Tight::new(&TIGHT_BUS)), move |_| shared).await?;
        // The first entry fills the one slot of the audit log for good.
        tb.broker::<Tight>()
            .message(&Sent(&[1]))
            .to("audit-log")
            .publish()
            .await?;
        tb.broker::<Tight>()
            .message(&Sent(&[7]))
            .to("pressed")
            .publish()
            .await?;

        assert_eq!(kept.0.get(), Some(7));
        tb.shutdown().await?;
        Ok(())
    })
}
