use core::convert::Infallible;
use core::future::ready;

use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream_runtime::{
    Context, Deserialized, HandlerOutcome, Input, LocalHandle, SoloDeserialized, SubscriberSettings,
    local_subscriber,
};
use ruststream_runtime_embassy::{EmbeddedApp, Mounts};
use ruststream::nonzero;

struct Reading<'a>(&'a [u8]);

impl Deserialized for Reading<'_> {
    type Output<'a> = Reading<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> {
        Ok(Reading(payload))
    }
}

impl Input for Reading<'_> {
    type Axis = SoloDeserialized<Reading<'static>>;
}

struct Log;

impl LocalHandle<Reading<'_>> for Log {
    fn handle(
        &self,
        reading: &Reading<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = reading.0;
        ready(Ok(()))
    }
}

fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(
            local_subscriber(LocalMemorySource::new("readings"), Log)
                .threads(nonzero!(2))
                .build(),
        )
    })
}

fn main() {
    let _ = app(LocalMemoryBroker::new());
}
