use ruststream::codec::JsonCodec;
use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{BytesMut, Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

// A type that owns its byte layout: the format decides how it is encoded, and the mount site
// does not get a say.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "orders.archived")]
#[wire(encode = write_archived)]
struct Archived {
    id: u32,
}

fn write_archived(archived: &Archived, buf: &mut BytesMut) {
    buf.extend_from_slice(&archived.id.to_be_bytes());
}

#[derive(OutSlot)]
#[publishes(Archived)]
struct Events;

// Naming a codec for such a value does not compile: it would be silently ignored, so the
// position does not exist on this wire.
#[subscriber("orders")]
async fn forward(
    frame: &Frame<'_>,
    Out(out): Out<impl Publisher, Events, Archived>,
) -> HandlerOutcome {
    let _ = out
        .message(&Archived {
            id: frame.0.len() as u32,
        })
        .with_codec(JsonCodec)
        .publish()
        .await;
    HandlerOutcome::ack()
}

fn main() {}
