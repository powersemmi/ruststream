use core::convert::Infallible;
use core::future::{Future, ready};

use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, MountSteps, Mounts, Outcomes, SubscriberSettings};
use ruststream::{
    ConnectedBroker, Outgoing, OutgoingMessage, PairError, PayloadBuf, PublishOutcome,
    PublishPolicy, Publisher, Take, subscriber,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Reading {
    celsius: i32,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Answer {
    celsius: i32,
}

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
async fn answer(readings: &[Reading]) -> Outcomes<Answer, 3> {
    readings
        .iter()
        .map(|reading| Answer { celsius: reading.celsius })
        .collect()
}

// The reply publisher keeps its payload, so the worker's room past 256 bytes would go unused.
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.with_codec(PostcardCodec)
            .include(answer.batch::<3>())
            .out_reply(Keep)
            .codec(PostcardCodec)
            .encode_capacity::<512>()
    })
}

fn main() {}
