//! A codec of your own over a serde format: `MessagePack` with field names, the form a peer in a
//! dynamic language writes and reads. The scenario is a test.
//!
//! ```text
//! cargo test --example custom_codec --no-default-features --features testing,macros,memory,msgpack
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use bytes::BufMut;
use rmp_serde::encode;
use ruststream::codec::{Codec, CodecError};
use ruststream::memory::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

/// Bytes published as themselves: how the test plays the peer.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{NAMED, NamedMsgpack, Order, Receipt, Wire};

    #[subscriber("orders", reply("receipts"))]
    async fn checkout(order: &Order) -> Receipt {
        tracing::info!(order.id, "checked out");
        Receipt { id: order.id }
    }

    // --8<-- [start:mount]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            NamedMsgpack,
            |b| b.include(checkout).out_reply(Publish).codec(NamedMsgpack),
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
