//! The scenarios of the no-alloc publish stand: the handlers that publish and what they publish,
//! shared by the benchmark and its check.

use std::cell::Cell;

use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use serde::Serialize;

/// The bus: each subscription holds four messages, and there is room for sixteen subscriptions.
pub(crate) type Bus = LocalMemoryBroker<4, 64, 16>;

thread_local! {
    /// Deliveries the publishing subscription handled.
    pub(crate) static HANDLED: Cell<usize> = const { Cell::new(0) };
}

/// One reading as its bytes.
#[derive(Deserialized)]
pub(crate) struct Reading<'a>(&'a [u8]);

/// The reading the handler passes on: its first byte.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "relayed")]
pub(crate) struct Relayed([u8; 1]);

/// The header contract of a traced reading.
#[derive(Serialize)]
pub(crate) struct Trace {
    trace: &'static str,
}

/// The reading the handler passes on with the trace of its origin.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "relayed", headers = Trace)]
pub(crate) struct Traced([u8; 1]);

/// The slot the readings leave through.
#[derive(OutSlot)]
#[publishes(Relayed, Traced)]
pub(crate) struct Relay;

/// The answer to a reading, to a destination nothing reads.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
pub(crate) struct Answer([u8; 1]);

/// The handler a scenario mounts.
#[derive(Clone, Copy)]
pub(crate) enum Publish {
    ThroughASlot,
    WithAHeader,
    AsAReply,
}

/// The first byte of a reading.
fn first(reading: &Reading<'_>) -> [u8; 1] {
    [reading.0.first().copied().unwrap_or_default()]
}

fn handled() {
    HANDLED.with(|handled| handled.set(handled.get() + 1));
}

#[subscriber(LocalMemorySource::new("busy"))]
pub(crate) async fn relay(
    reading: &Reading<'_>,
    Out(relay): Out<impl Publisher, Relay>,
) -> HandlerOutcome {
    let sent = relay.message(&Relayed(first(reading))).publish().await;
    handled();
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[subscriber(LocalMemorySource::new("busy"))]
pub(crate) async fn relay_traced(
    reading: &Reading<'_>,
    Out(relay): Out<impl Publisher, Relay>,
) -> HandlerOutcome {
    let sent = relay
        .message(&Traced(first(reading)))
        .with_headers(&Trace { trace: "boiler" })
        .publish()
        .await;
    handled();
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[subscriber(LocalMemorySource::new("busy"), reply)]
pub(crate) async fn answer(reading: &Reading<'_>) -> Answer {
    handled();
    Answer(first(reading))
}
