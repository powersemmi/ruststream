use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Out, RustStream};
use ruststream::{OutSlot, Publisher, subscriber};

#[derive(OutSlot)]
struct Encoded;

#[derive(OutSlot)]
struct Audit;

#[subscriber("chunks")]
async fn transcode(
    chunk: &[u8],
    Out(_encoded): Out<impl Publisher, Encoded>,
    Out(_audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let _ = chunk;
    HandlerOutcome::ack()
}

fn main() {
    // The Audit slot is never bound: `.build()` must not compile, and the error names the
    // missing slot through `MissingSlot<Audit>` in the attachment type.
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(transcode).out(Encoded, MemoryPublish).build();
    });
}
