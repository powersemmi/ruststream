//! A device service with no serde codec writes a handler with an `Out` slot that publishes the
//! types that carry their own bytes. The slot has no codec to encode with, and none is asked of
//! it: only an operation that encodes names one.
//!
//! Built where no codec feature is on and the Tokio runner is off, as a firmware crate builds
//! its facade: `cargo check --tests --no-default-features --features embassy,macros,memory`.
#![cfg(all(
    feature = "embassy",
    feature = "macros",
    feature = "memory",
    not(feature = "tokio"),
    not(any(feature = "json", feature = "cbor", feature = "msgpack"))
))]
#![allow(clippy::future_not_send)]

use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// A reading as its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

/// An alert: the reading's bytes, as they came.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bus/alerts")]
struct Alert(Vec<u8>);

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Raises an alert for every reading.
#[subscriber(LocalMemorySource::new("bus/levels"))]
async fn on_level(level: &Reading<'_>, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    match alerts.message(&Alert(level.0.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_level).out(Alerts, LocalMemoryPublish))
}

#[test]
fn a_codec_free_service_mounts_a_handler_with_an_out_slot() {
    let _ = app;
}
