use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, Mounts, Out, SubscriberSettings};
use ruststream::{OutSlot, Outgoing, Publisher, Serialized, nonzero, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Reading {
    celsius: u8,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Answer {
    celsius: u8,
}

#[derive(Outgoing, Serialized)]
#[outgoing(name = "audit-log")]
struct Entry([u8; 1]);

#[derive(OutSlot)]
#[publishes(Entry)]
struct Audit;

// A batch handler on the embedded runner cannot both reply and take an `Out` slot.
#[subscriber(LocalMemorySource::new("readings"), reply)]
async fn answer(readings: &[Reading], Out(_audit): Out<impl Publisher, Audit>) -> Vec<Answer> {
    readings
        .iter()
        .map(|reading| Answer {
            celsius: reading.celsius,
        })
        .collect()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(answer.batch(nonzero!(3)))
            .out(Audit, LocalMemoryPublish)
    })
}

fn main() {}
