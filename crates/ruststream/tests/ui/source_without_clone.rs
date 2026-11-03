use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError, MemorySource, MemorySubscriber};
use ruststream::prelude::*;
use ruststream::runtime::{AppInfo, IntoSource, RustStream, subscriber};
use ruststream::{BrokerMoves, Deserialized, SubscriptionSource};

// A descriptor of the service's own, with no `Clone`: the error names it once, at the call that
// takes it, and the mount below adds nothing.
struct Sensor(MemorySource);

impl IntoSource for Sensor {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for Sensor {
    type Subscriber = MemorySubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        SubscriptionSource::<ConnectedMemoryBroker>::name(&self.0)
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<MemorySubscriber, MemoryError> {
        self.0.subscribe(connected).await
    }
}

#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

struct Ingest;

impl Handle<Frame<'_>> for Ingest {
    async fn handle(
        &self,
        frame: &Frame<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let _ = frame.0.len();
        Ok(())
    }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber(Sensor(MemorySource::new("readings")), Ingest).build())
    });
}
