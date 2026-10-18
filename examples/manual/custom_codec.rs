//! The service of `examples/custom_codec.rs` written without the `macros` feature: the codec is
//! the same, and the registration names it at the same two positions.
//!
//! ```text
//! cargo test --example manual_custom_codec --no-default-features --features testing,memory,msgpack
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::convert::Infallible;

use bytes::BufMut;
use rmp_serde::encode;
use ruststream::codec::{Codec, CodecError};
use ruststream::memory::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

/// Bytes published as themselves: how the test plays the peer. What
/// `#[derive(Outgoing, Serialized)]` writes, by hand.
struct Wire(Vec<u8>);

impl Serialized for Wire {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

/// `{"id": 7}` as a `MessagePack` map, the way the peer writes it.
const NAMED: &[u8] = b"\x81\xa2id\x07";

// --8<-- [start:codec]
/// `MessagePack` that writes a struct as a map of field names, not as an array of values.
#[derive(Debug, Clone, Copy, Default)]
struct NamedMsgpack;

impl Codec for NamedMsgpack {
    const CONTENT_TYPE: &'static str = "application/msgpack";

    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let mut buf = BytesMut::new();
        self.encode_into(value, &mut buf)?;
        Ok(buf)
    }

    fn encode_into<T: Serialize>(&self, value: &T, buf: &mut BytesMut) -> Result<(), CodecError> {
        // Straight into the publish path's buffer: nothing else is allocated.
        encode::write_named(&mut buf.writer(), value)
            .map_err(|err| CodecError::Encode(Box::new(err)))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        rmp_serde::from_slice(bytes).map_err(|err| CodecError::Decode(Box::new(err)))
    }
}
// --8<-- [end:codec]

mod service {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{NAMED, NamedMsgpack, Order, Receipt, Wire};

    struct Checkout;

    impl Handle<Order, Receipt> for Checkout {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            tracing::info!(order.id, "checked out");
            ready(Ok(Receipt { id: order.id }))
        }
    }

    // --8<-- [start:mount]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            NamedMsgpack,
            |b| {
                b.include(
                    subscriber("orders", Checkout)
                        .reply()
                        .to("receipts")
                        .build(),
                )
                .out_reply(Publish)
                .codec(NamedMsgpack);
            },
        )
    }
    // --8<-- [end:mount]

    #[tokio::test]
    async fn the_reply_travels_as_a_map() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .message(&Wire(NAMED.to_vec()))
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with_codec(&NamedMsgpack, &Order { id: 7 })
            .settled(HandlerOutcome::ack());
        // The receipt `{"id": 7}` is the same map, byte for byte.
        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with_raw(NAMED);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
