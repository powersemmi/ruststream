//! The Protobuf service written without the `macros` feature: the same generated message on the
//! same byte lanes, with every impl the derives would have emitted spelled out.
//!
//! It doubles as the answer to what `#[wire(prost)]` expands to. The attribute is not a plugin
//! system and knows nothing about Protobuf: it fills two function paths into the impls below, and
//! that is the whole of it. A format the shorthand does not cover is hand-rolled exactly this
//! way: name its own encoder in `wire_bytes` and its own reader in `from_payload`, and the type
//! is on the lanes.
//!
//! Note the feature list: no `macros`, and no codec either. Neither lane resolves one.
//!
//! ```text
//! cargo run --example manual_protobuf --no-default-features --features memory
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;

// --8<-- [start:message]
/// What `prost-build` emits from the schema. On the macro path a `message_attribute` line adds
/// `#[derive(Outgoing, Serialized, Deserialized)]` and `#[wire(prost)]` to it; here the impls
/// below take their place, and the struct is what the generator wrote and nothing more.
#[derive(Clone, PartialEq, prost::Message)]
struct Order {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    sku: String,
}

// `#[derive(Serialized)]` under `#[wire(encode = ..)]`: the format's own encoder writes into the
// buffer the publish path already carries, and the value lends what it wrote - so the message is
// encoded once and nothing intermediate is allocated. The error is the format's, named here
// because a hand-written impl can see it (the derive cannot, and erases it instead).
impl Serialized for Order {
    type Error = prost::EncodeError;

    fn wire_bytes<'a>(&'a self, buf: &'a mut BytesMut) -> Result<&'a [u8], Self::Error> {
        prost::Message::encode(self, &mut *buf)?;
        Ok(&buf[..])
    }
}

// The two spellings that route the type onto the serialized wire. They carry no bytes of their
// own: what differs between a byte bag and a generated message is `wire_bytes`, not where the
// value may be used, so the derive writes these two the same way for both.
impl MessageWire for Order {
    type Wire = SerializedWire;
}

impl ReplyShape for Order {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

// `#[derive(Deserialized)]` under `#[wire(decode = ..)]`, plus the `Input` spelling that routes
// `&Order` onto the self-deserializing lane. `Output<'a>` is `Self` because a generated message
// owns its fields; a zero-copy reader names its own borrowing view there instead.
impl Deserialized for Order {
    type Output<'a> = Self;
    type Error = prost::DecodeError;

    fn from_payload(payload: &[u8]) -> Result<Self, Self::Error> {
        prost::Message::decode(payload)
    }
}

impl Input for Order {
    type Axis = SoloDeserialized<Self>;
}

// `#[derive(Outgoing)]` with `#[outgoing(name = "orders")]`: where the type goes, what header
// contract it declares, and what it contributes to the generated document. `with_serialized`
// records the lane, which is what the derive's probe reports for a type carrying its own bytes.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const ADDRESS: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl<M: OutSlot> OutMessages<M> for Order {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(Self::ADDRESS, std::any::type_name::<Self>())
                .with_serialized(true),
        ]
    }
}
// --8<-- [end:message]

// --8<-- [start:handler]
/// The definition value `#[subscriber("orders")]` would have minted. The body reads the model
/// type, not bytes: the lane decoded the payload with the message's own reader before the call.
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("order {} of {}", order.id, order.sku);
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Receive).build());
        // The outgoing half of the same declaration: the value encodes itself on the way out
        // too, so this publish names no codec either.
        b.after_startup(Publish, async move |publisher| {
            publisher
                .message(&Order {
                    id: 7,
                    sku: "widget".to_owned(),
                })
                .publish()
                .await
        });
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:app]
