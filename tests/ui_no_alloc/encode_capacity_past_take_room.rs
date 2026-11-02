use core::convert::Infallible;
use core::future::{Future, ready};

use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, MountSteps, Mounts};
use ruststream::{
    ConnectedBroker, Deserialized, Outgoing, OutgoingMessage, PairError, PayloadBuf, PublishOutcome,
    PublishPolicy, Publisher, Serialized, Take, subscriber,
};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(&'static [u8]);

// A transport that keeps the payload it is handed.
#[derive(Clone)]
struct Keeping;

impl Publisher for Keeping {
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    fn publish<'msg>(
        &self,
        _msg: OutgoingMessage<'msg, PayloadBuf>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, Infallible, Take>> {
        ready(Ok(()))
    }
}

struct Keep;

impl<Connected: ConnectedBroker> PublishPolicy<Connected> for Keep {
    type Live = Keeping;

    fn pair(self, _connected: &Connected) -> impl Future<Output = Result<Keeping, PairError>> {
        ready(Ok(Keeping))
    }
}

#[subscriber(LocalMemorySource::new("questions"), reply)]
async fn answer(_question: &Reading<'_>) -> Answer {
    Answer(b"yes")
}

// The reply publisher keeps its payload, so the lane's room past 256 bytes would go unused.
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(answer).out_reply(Keep).encode_capacity::<512>())
}

fn main() {}
