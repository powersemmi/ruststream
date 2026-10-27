//! Several message types on one subscription, written without the `macros` feature: the set is
//! an enum with the `InMessages` impl spelled out, and the handler is an `impl Handle` over it.
//!
//! The rule is the one `#[in_messages(header = "event")]` writes: the `event` header names the
//! model, and each model decodes through its own path (`decode_variant`), the codec of the
//! mount for the JSON model and the Protobuf reader for the generated one.
//!
//! ```text
//! cargo test --example manual_in_messages --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::convert::Infallible;
use std::error::Error;
use std::future::ready;

use ruststream::codec::{Codec, CodecError};
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

// --8<-- [start:models]
/// An order was placed.
#[derive(Debug, Serialize, Deserialize)]
struct OrderCreated {
    id: u64,
}

impl MessageInfo for OrderCreated {
    const NAME: &'static str = "OrderCreated";
    const DESCRIPTION: Option<&'static str> = Some("An order was placed.");
}

/// An order was cancelled, sent as Protobuf by another service. What `prost-build` emits; the
/// two impls below are what `#[derive(Deserialized)]` with `#[wire(prost)]` writes.
#[derive(Clone, PartialEq, prost::Message)]
struct OrderCancelled {
    #[prost(uint64, tag = "1")]
    id: u64,
}

impl MessageInfo for OrderCancelled {
    const NAME: &'static str = "OrderCancelled";
    const DESCRIPTION: Option<&'static str> =
        Some("An order was cancelled, sent as Protobuf by another service.");
}

impl Deserialized for OrderCancelled {
    type Output<'a> = Self;
    type Error = prost::DecodeError;

    fn from_payload(payload: &[u8]) -> Result<Self, Self::Error> {
        prost::Message::decode(payload)
    }
}

impl Input for OrderCancelled {
    type Axis = SoloDeserialized<Self>;
}
// --8<-- [end:models]

// --8<-- [start:set]
/// Every event of an order on the `orders` subscription: the `event` header names the model.
enum OrderEvent {
    Created(OrderCreated),
    Cancelled(OrderCancelled),
}

/// The kinds of the set, what a discriminator names; the derive generates this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OrderEventKind {
    Created,
    Cancelled,
}

impl InMessages for OrderEvent {
    type Kind = OrderEventKind;

    fn decode<DecodeCodec, M>(codec: &DecodeCodec, msg: &M) -> Result<Self, CodecError>
    where
        DecodeCodec: Codec,
        M: IncomingMessage,
    {
        let kind = match msg.headers().get("event") {
            Some(b"created") => OrderEventKind::Created,
            Some(b"cancelled") => OrderEventKind::Cancelled,
            Some(other) => {
                return Err(CodecError::Decode(
                    format!(
                        "the `event` header is `{}`, which names no variant of `OrderEvent`",
                        String::from_utf8_lossy(other)
                    )
                    .into(),
                ));
            }
            None => {
                return Err(CodecError::Decode(
                    "the delivery carries no `event` header".into(),
                ));
            }
        };
        // The chosen variant decodes once, through its model's own path.
        let payload = msg.payload();
        match kind {
            OrderEventKind::Created => {
                <OrderCreated as InVariant<Self>>::decode_variant(codec, payload)
                    .map(OrderEvent::Created)
            }
            OrderEventKind::Cancelled => {
                <OrderCancelled as InVariant<Self>>::decode_variant(codec, payload)
                    .map(OrderEvent::Cancelled)
            }
        }
    }

    // One entry per model, in the order of the variants, each with the header value that names
    // it; the document lists them as the messages of the subscription.
    fn incoming() -> Vec<IncomingMessageMetadata> {
        vec![
            <OrderCreated as InVariant<Self>>::describe()
                .with_discriminator(HeaderDiscriminator::new("event", "created")),
            <OrderCancelled as InVariant<Self>>::describe()
                .with_discriminator(HeaderDiscriminator::new("event", "cancelled")),
        ]
    }
}

impl Input for OrderEvent {
    type Axis = SoloSet<Self>;
}
// --8<-- [end:set]

// --8<-- [start:handler]
/// The definition value `#[subscriber("orders")]` would have minted: the body reads the set,
/// decoded as the variant the header named before the call.
struct OnEvent;

impl Handle<OrderEvent> for OnEvent {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        match event {
            OrderEvent::Created(created) => tracing::info!(created.id, "created"),
            OrderEvent::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
        }
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", OnEvent).build());
    })
}
// --8<-- [end:app]

// The scenario is the test below; the command in the header runs it.
fn main() {}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;

    /// The header the set reads, as the typed-headers publish of the harness writes it.
    #[derive(Serialize)]
    struct EventHeader {
        event: &'static str,
    }

    /// Protobuf bytes published as themselves under the `event` header: what
    /// `#[derive(Outgoing, Serialized)]` with `#[outgoing(headers = EventHeader)]` writes.
    struct Tagged(Vec<u8>);

    impl Serialized for Tagged {
        type Error = Infallible;

        fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Self::Error> {
            Ok(WireBytes::Own(&self.0))
        }
    }

    impl MessageWire for Tagged {
        type Wire = SerializedWire;
    }

    impl OutgoingDestination for Tagged {
        type Form = CallerName;
        const DESTINATION: &'static str = "";
    }

    impl MessageHeaders for Tagged {
        type Contract = WithHeaders<EventHeader>;
    }

    #[tokio::test]
    async fn the_header_names_the_model() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .publish_with_headers(
                "orders",
                &OrderCreated { id: 1 },
                &EventHeader { event: "created" },
            )
            .await?;
        broker
            .message(&Tagged(OrderCancelled { id: 2 }.encode_to_vec()))
            .to("orders")
            .with_headers(&EventHeader { event: "cancelled" })
            .publish()
            .await?;
        tb.settle().await?;

        broker
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
