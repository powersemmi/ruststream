use ruststream::BytesMut;
use ruststream::codec::{Codec, CodecError, FrameNames, JsonCodec};
use ruststream::memory::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// JSON behind a schema id that names one record.
#[derive(Clone)]
struct Registry;

impl Codec for Registry {
    type Frame<'a> = u8;

    fn frame<'a>(&'a self, payload: &'a [u8]) -> Result<Self::Frame<'a>, CodecError> {
        Ok(payload.first().copied().unwrap_or_default())
    }

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

#[derive(Deserialize, MessageInfo)]
struct OrderCancelled {
    id: u64,
}

// The registry knows the created record, and nothing else.
impl FrameNames<OrderCreated> for Registry {
    fn names(frame: &u8) -> bool {
        *frame == 1
    }
}

#[derive(InMessages)]
#[in_messages(by_codec)]
enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

#[subscriber("orders")]
async fn on_event(_event: &OrderEvent) -> HandlerOutcome {
    HandlerOutcome::ack()
}

// The codec names `OrderCreated` but not `OrderCancelled`: the error names the second model.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        Registry,
        |b| b.include(on_event),
    )
}

fn main() {
    let _ = app();
}
