use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, OutTransform, Outgoing, RustStream};
use ruststream::{Deserialized, OutSlot, Publisher, subscriber};

#[derive(Deserialized)]
struct Chunk<'a>(&'a [u8]);

#[derive(OutSlot)]
struct Encoded;

struct Envelope;

impl OutTransform for Envelope {
    fn apply(&self, out: &mut Outgoing<'_>) {
        out.headers_mut().insert("x-outbox", b"1".to_vec());
    }
}

#[subscriber("chunks")]
async fn transcode(chunk: &Chunk<'_>, Out(_encoded): Out<impl Publisher, Encoded>) -> HandlerOutcome
{
    let _ = chunk.0;
    HandlerOutcome::ack()
}

fn main() {
    // The transform names no slot of its own: it rides the `.out(marker, policy)` call before
    // it, and there is none here.
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(transcode)
            .transform(Envelope)
            .out(Encoded, MemoryPublish)
            .build();
    });
}
