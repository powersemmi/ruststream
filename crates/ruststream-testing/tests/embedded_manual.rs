//! A device service declared without the macros, tested on the host through `TestApp`: the test
//! bodies are those of `embedded.rs`, since the harness reads the same records whichever way the
//! service was declared.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use embassy_futures::block_on;
use ruststream::memory::local::prelude::{
    BytesMut, LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    Context, Deserialized, EmbeddedApp, HandlerOutcome, Input, LocalHandle, LocalOutEntry,
    MessageWire, Mounts, OutSlot, Outs, PublishedThrough, Reads, Serialized, SerializedWire,
    SoloDeserialized, WireBytes, local_subscriber,
};
use ruststream::testing::embedded::TestApp;
use ruststream::{
    CallerName, FixedName, LocalPublisher, MessageHeaders, NoHeaders, OutgoingDestination,
};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A sensor's frame, as its bytes.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = fmt::Error;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, fmt::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// An alert, carrying the level's bytes as they came.
struct Alert(Vec<u8>);

// `#[derive(Outgoing, Serialized)]` with `#[outgoing(name = "alerts")]`, by hand.
impl OutgoingDestination for Alert {
    type Form = FixedName;

    const DESTINATION: &'static str = "alerts";
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
}

impl Serialized for Alert {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Alert {
    type Wire = SerializedWire;
}

/// The slot alerts leave through.
struct Alerts;

impl OutSlot for Alerts {
    const NAME: &'static str = "Alerts";
    type Destination = Reads;
}

impl PublishedThrough<Alerts> for Alert {}

/// What the board's handlers read: the level from which a reading is an alarm.
struct Limits {
    alarm: u8,
}

/// Raises an alert for every level at or above the board's limit.
struct Raise;

impl<Entry> LocalHandle<Frame<'_>, (), Outs<(Entry,)>, (), Limits> for Raise
where
    Entry: LocalOutEntry<Alerts, Wire: LocalPublisher>,
{
    async fn handle(
        &self,
        level: &Frame<'_>,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_, (), Limits>,
    ) -> Result<(), HandlerOutcome> {
        if level.0.first().copied().unwrap_or(0) < ctx.state().alarm {
            return Ok(());
        }
        outs.get(Alerts)
            .message(&Alert(level.0.to_vec()))
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

/// The tank's app, with the limits the board reads from its flash.
fn tank(bus: Bus) -> EmbeddedApp<impl Mounts<State = Limits>> {
    EmbeddedApp::new()
        .with_state(Limits { alarm: 200 })
        .with_broker(bus, |b| {
            b.include(local_subscriber(LocalMemorySource::new("tank/level"), Raise).build())
                .out(Alerts, LocalMemoryPublish)
        })
}

/// A frame the test publishes, to the destination it names.
struct Sent(&'static [u8]);

// `#[derive(Outgoing, Serialized)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Sent {
    type Form = CallerName;
}

impl MessageHeaders for Sent {
    type Contract = NoHeaders;
}

impl Serialized for Sent {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(self.0))
    }
}

impl MessageWire for Sent {
    type Wire = SerializedWire;
}

// --8<-- [start:out]
#[test]
fn an_alarm_goes_through_its_slot() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::with_state(tank(Bus::new()), |_brokers| Limits { alarm: 50 }).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"\x64"))
            .to("tank/level")
            .publish()
            .await?;

        tb.out::<Alerts>().assert_called_once().with_raw(b"\x64");
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:out]

// --8<-- [start:on-publish]
#[test]
fn a_hook_checks_each_alarm_as_it_goes_out() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::with_state(tank(Bus::new()), |_brokers| Limits { alarm: 50 }).await?;
        let hooked = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hooked);
        tb.out::<Alerts>().on_publish(async move |alerts| {
            alerts.assert_called_once().with_raw(b"\x64");
            seen.fetch_add(1, Ordering::SeqCst);
        });

        tb.broker::<Bus>()
            .message(&Sent(b"\x64"))
            .to("tank/level")
            .publish()
            .await?;
        assert_eq!(hooked.load(Ordering::SeqCst), 1);
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:on-publish]

// --8<-- [start:with-state]
#[test]
fn the_board_alarms_from_the_tests_limit() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(tank(Bus::new())).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"\x64"))
            .to("tank/level")
            .publish()
            .await?;
        tb.out::<Alerts>().assert_not_called();
        tb.shutdown().await?;

        let tb = TestApp::with_state(tank(Bus::new()), |_brokers| Limits { alarm: 50 }).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"\x64"))
            .to("tank/level")
            .publish()
            .await?;
        tb.out::<Alerts>().assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
// --8<-- [end:with-state]
