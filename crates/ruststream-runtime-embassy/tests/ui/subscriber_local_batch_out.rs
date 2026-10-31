use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(Vec<u8>);

#[derive(OutSlot)]
struct Audit;

// A batch handler on the embedded runner cannot take an `Out` slot.
#[subscriber(LocalMemorySource::new("readings"))]
async fn bill(readings: &[Reading<'_>], Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = readings.len();
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(bill).out(Audit, LocalMemoryPublish))
}

fn main() {}
