use ruststream::codec::Codec;
use ruststream::memory::prelude::*;
use serde::Deserialize;

/// What a registry codec answers beside decoding: the record a schema id stands for.
trait RecordNames: Codec {
    fn record_name(&self, schema: &[u8]) -> Option<&'static str>;
}

#[derive(Deserialize, MessageInfo)]
struct OrderCreated {
    id: u64,
}

#[derive(InMessages)]
#[in_messages(discriminate_with_codec = ByRecord)]
enum OrderEvent {
    Created(OrderCreated),
}

/// Answers with any codec that names records.
struct ByRecord;

impl<DecodeCodec: RecordNames> CodecDiscriminator<OrderEventKind, DecodeCodec> for ByRecord {
    fn kind<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Option<OrderEventKind> {
        codec.record_name(msg.payload()).map(|_| OrderEventKind::Created)
    }
}

#[subscriber("orders")]
async fn on_event(_event: &OrderEvent) -> HandlerOutcome {
    HandlerOutcome::ack()
}

// The default codec is JSON, which names no records: the set does not mount here.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(on_event);
    })
}

fn main() {
    let _ = app();
}
