// A batch handler on the embedded runner takes an `Out` slot and publishes through it, as a
// handler of one message does.
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out, SubscriberSettings};
use ruststream::{OutSlot, Outgoing, Publisher, Serialized, nonzero, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Reading {
    celsius: u8,
}

#[derive(Outgoing, Serialized)]
#[outgoing(name = "audit-log")]
struct Entry([u8; 1]);

#[derive(OutSlot)]
#[publishes(Entry)]
struct Audit;

#[subscriber(LocalMemorySource::new("readings"))]
async fn on_readings(
    readings: &[Reading],
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    for reading in readings {
        if audit
            .message(&Entry([reading.celsius]))
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_readings.batch(nonzero!(3)))
            .out(Audit, LocalMemoryPublish)
    })
}

fn main() {}
