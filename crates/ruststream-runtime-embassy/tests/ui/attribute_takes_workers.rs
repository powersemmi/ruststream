use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
use ruststream::{Deserialized, subscriber};
use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream_runtime_embassy::{EmbeddedApp, Mounts};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[subscriber(LocalMemorySource, workers(4))]
async fn log(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0.len();
    HandlerOutcome::ack()
}

fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(log.name("readings")))
}

fn main() {
    let _ = app(LocalMemoryBroker::new());
}
