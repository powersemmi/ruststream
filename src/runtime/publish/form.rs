//! What the runtime does with a payload on each side of a publisher's declaration.
//!
//! [`PayloadForm`] says what a transport is handed; these are its other half, the framework's
//! own: where the encode writes, how the pipeline carries the result, and what the message that
//! leaves is built from. One implementation per form, chosen by the publisher's type, so no
//! publish position ever asks which form it got.

use std::marker::PhantomData;

use bytes::BytesMut;
use serde::Serialize;

use super::{Outgoing, OutgoingName, Payload, Serialized, WireBytes};
use crate::codec::{Codec, CodecError};
use crate::{HeaderMap, Lend, OutgoingMessage, PayloadForm, Take};

/// The destination as the message that leaves reads it, kept in the spent stage's own slot
/// where the stage owns it.
///
/// A name the mount site borrowed - the literal every `#[subscriber(.., publish("dest"))]`
/// carries - is already valid for the publish and is kept nowhere.
#[inline]
fn kept<'a>(name: OutgoingName<'a>, slot: &'a mut Option<OutgoingName<'static>>) -> &'a str {
    match name {
        OutgoingName::Borrowed(name) => name,
        OutgoingName::Owned(name) => slot.insert(OutgoingName::Owned(name)).as_str(),
        OutgoingName::Shared(name) => slot.insert(OutgoingName::Shared(name)).as_str(),
    }
}

impl PayloadForm for Lend {
    type Form<'a> = &'a [u8];

    // The codec writes into the publish path's own buffer, so the path carries it.
    type Encode<'a> = &'a mut BytesMut;

    // The destination where the stage owns it, and the buffer where a stage above the leaf
    // wrote one: the transport reads both, so both outlive the send.
    type Spent = (Option<OutgoingName<'static>>, Option<BytesMut>);

    #[inline]
    fn encode_slot<'a>(buf: &'a mut BytesMut) -> &'a mut BytesMut {
        buf
    }

    #[inline]
    fn encode_again<'s, 'a: 's>(slot: &'s mut &'a mut BytesMut) -> &'s mut BytesMut {
        slot
    }

    #[inline]
    fn encoded<'b, C, T>(
        codec: &C,
        value: &T,
        buf: Self::Encode<'b>,
    ) -> Result<Self::Form<'b>, CodecError>
    where
        C: Codec,
        T: Serialize,
    {
        buf.clear();
        codec.encode_into(value, buf)?;
        Ok(&buf[..])
    }

    #[inline]
    fn serialized<'v, T>(value: &'v T, buf: Self::Encode<'v>) -> Result<Self::Form<'v>, T::Error>
    where
        T: Serialized,
    {
        buf.clear();
        // Both answers are lent as they are: the transport reads them and keeps neither.
        Ok(value.wire_bytes(buf)?.of(buf))
    }

    #[inline]
    fn rebuilt<'a>(name: OutgoingName<'a>, payload: &'a [u8], headers: HeaderMap) -> Outgoing<'a> {
        Outgoing::rebuilding(name, Payload::Lent(payload), headers)
    }

    #[inline]
    fn leaving<'a>(out: &'a mut Outgoing<'a>) -> OutgoingMessage<'a, &'a [u8]> {
        let headers = out.take_headers();
        // The mutable borrow ends here; what is left is read, and read for as long as the
        // message that leaves lives.
        let out: &'a Outgoing<'a> = out;
        OutgoingMessage::assembled(out.name(), out.payload(), headers)
    }

    #[inline]
    fn handed_on<'a>(
        out: Outgoing<'a>,
        spent: &'a mut Self::Spent,
    ) -> OutgoingMessage<'a, &'a [u8]> {
        let (name, payload, headers) = out.into_parts();
        let (kept_name, kept_buffer) = spent;
        let bytes = match payload {
            Payload::Lent(bytes) => bytes,
            // A stage above wrote a buffer of its own; the transport only reads it, so it stays
            // with the caller until the publish is over.
            Payload::Owned(buf) => &kept_buffer.insert(buf)[..],
        };
        OutgoingMessage::assembled(kept(name, kept_name), bytes, headers)
    }
}

impl PayloadForm for Take {
    type Form<'a> = BytesMut;

    // The codec produces the buffer the transport keeps, so there is nothing to write into: a
    // publish on this form carries the scratch's borrow and none of its bytes, which is what
    // keeps it out of every future on the way.
    type Encode<'a> = PhantomData<&'a mut BytesMut>;

    // The destination alone, and only where the stage owns it: the payload and the map move
    // into the message the transport keeps.
    type Spent = Option<OutgoingName<'static>>;

    #[inline]
    fn encode_slot<'a>(_buf: &'a mut BytesMut) -> PhantomData<&'a mut BytesMut> {
        PhantomData
    }

    #[inline]
    fn encode_again<'s, 'a: 's>(
        _slot: &'s mut PhantomData<&'a mut BytesMut>,
    ) -> PhantomData<&'s mut BytesMut> {
        PhantomData
    }

    #[inline]
    fn encoded<'b, C, T>(
        codec: &C,
        value: &T,
        _buf: Self::Encode<'b>,
    ) -> Result<Self::Form<'b>, CodecError>
    where
        C: Codec,
        T: Serialize,
    {
        // The transport keeps what it is handed, so the codec's own buffer travels to it and the
        // caller's scratch is never touched.
        codec.encode(value)
    }

    #[inline]
    fn serialized<'v, T>(value: &'v T, _buf: Self::Encode<'v>) -> Result<Self::Form<'v>, T::Error>
    where
        T: Serialized,
    {
        // A value that writes its bytes writes them into the buffer the transport will keep,
        // which is this one; a value that holds them is copied into it.
        let mut buf = BytesMut::new();
        match value.wire_bytes(&mut buf)? {
            // The value wrote into this buffer and nothing else holds it, so the transport is
            // handed it whole.
            WireBytes::InBuffer => Ok(buf),
            // The bytes belong to the value, which outlives neither the publish nor the
            // transport's claim on them, so this is the one copy the form asks for.
            WireBytes::Own(bytes) => Ok(BytesMut::from(bytes)),
        }
    }

    #[inline]
    fn rebuilt<'a>(name: OutgoingName<'a>, payload: BytesMut, headers: HeaderMap) -> Outgoing<'a> {
        Outgoing::rebuilding(name, Payload::Owned(payload), headers)
    }

    #[inline]
    fn leaving<'a>(out: &'a mut Outgoing<'a>) -> OutgoingMessage<'a, BytesMut> {
        let payload = out.take_payload();
        let headers = out.take_headers();
        OutgoingMessage::assembled(out.name(), payload, headers)
    }

    #[inline]
    fn handed_on<'a>(
        out: Outgoing<'a>,
        spent: &'a mut Self::Spent,
    ) -> OutgoingMessage<'a, BytesMut> {
        // The payload and the map move into the message the transport keeps, so the stage is
        // left holding the destination and nothing else.
        let (name, payload, headers) = out.into_parts();
        let payload = match payload {
            Payload::Lent(bytes) => BytesMut::from(bytes),
            Payload::Owned(buf) => buf,
        };
        OutgoingMessage::assembled(kept(name, spent), payload, headers)
    }
}
