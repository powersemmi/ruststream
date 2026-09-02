use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream};
use ruststream::{Deserialized, OutSlot, Publisher, subscriber};

#[derive(Deserialized)]
struct Chunk<'a>(&'a [u8]);

#[derive(OutSlot)]
struct Encoded;

#[derive(OutSlot)]
struct Audit;

#[subscriber("chunks")]
async fn transcode(
    chunk: &Chunk<'_>,
    Out(_encoded): Out<impl Publisher, Encoded>,
    Out(_audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let _ = chunk.0;
    HandlerOutcome::ack()
}

fn main() {
    // The Audit slot is never bound: `.build()` must not compile, and the error names the
    // missing slot through `MissingSlot<Audit>` in the attachment type.
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(transcode).out(Encoded, MemoryPublish).build();
    });
}
