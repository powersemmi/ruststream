use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out, SubscriberSettings};
use ruststream::{Deserialized, OutSlot, Publisher, subscriber};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[derive(OutSlot)]
struct Audit;

#[subscriber(LocalMemorySource, workers(4))]
async fn log(reading: &Reading<'_>, Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = reading.0.len();
    HandlerOutcome::ack()
}

fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(log.name("readings")).out(Audit, LocalMemoryPublish)
    })
}

fn main() {
    let _ = app(LocalMemoryBroker::new());
}
