use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, Mounts, Out};
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(Vec<u8>);

#[derive(OutSlot)]
struct Audit;

// A handler with an `Out` slot cannot reply on the embedded runner.
#[subscriber(LocalMemorySource::new("questions"), reply)]
async fn answer(question: &Reading<'_>, Out(_audit): Out<impl Publisher, Audit>) -> Answer {
    Answer(question.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(answer).out(Audit, LocalMemoryPublish))
}

fn main() {}
