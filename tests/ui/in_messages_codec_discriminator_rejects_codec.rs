use ruststream::BytesMut;
use ruststream::codec::{Codec, CodecError, JsonCodec};
use ruststream::memory::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// JSON whose records a registry names.
#[derive(Clone)]
struct Registry;

impl Codec for Registry {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        JsonCodec.encode(value)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        JsonCodec.decode(bytes)
    }
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

/// Answers with the registry codec alone.
struct ByRecord;

impl CodecDiscriminator<OrderEventKind, Registry> for ByRecord {
    fn kind<M: IncomingMessage>(_codec: &Registry, _msg: &M) -> Option<OrderEventKind> {
        Some(OrderEventKind::Created)
    }
}

#[subscriber("orders")]
async fn on_event(_event: &OrderEvent) -> HandlerOutcome {
    HandlerOutcome::ack()
}

// The default codec is JSON, which `ByRecord` cannot read: the set does not mount here.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(on_event))
}

fn main() {
    let _ = app();
}
