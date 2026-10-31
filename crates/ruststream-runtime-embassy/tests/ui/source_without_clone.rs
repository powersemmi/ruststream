use core::convert::Infallible;
use core::future::ready;

use ruststream_memory::local::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError, LocalMemorySource,
    LocalMemorySubscriber,
};
use ruststream_runtime::{
    Context, Deserialized, HandlerOutcome, Input, LocalHandle, SoloDeserialized, local_subscriber,
};
use ruststream_runtime_embassy::{EmbeddedApp, Mounts};
use ruststream_core::{BrokerMoves, SubscriptionSource};
use ruststream_runtime::IntoSource;

/// A descriptor of the device's own, with no `Clone`.
struct Sensor(LocalMemorySource);

impl IntoSource for Sensor {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl<const CAP: usize> SubscriptionSource<ConnectedLocalMemoryBroker<CAP>> for Sensor {
    type Subscriber = LocalMemorySubscriber<CAP>;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        SubscriptionSource::<ConnectedLocalMemoryBroker<CAP>>::name(&self.0)
    }

    fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<CAP>,
    ) -> impl Future<Output = Result<LocalMemorySubscriber<CAP>, LocalMemoryError>> {
        self.0.subscribe(connected)
    }
}

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
        b.include(local_subscriber(Sensor(LocalMemorySource::new("readings")), Log).build())
    })
}

fn main() {
    let _ = app(LocalMemoryBroker::new());
}
