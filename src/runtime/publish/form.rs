//! What the runtime does with a payload on each side of a publisher's declaration.
//!
//! [`PayloadForm`] says what a transport is handed; these are its other half, the framework's
//! own: where the encode writes, how the pipeline carries the result, and what the message that
//! leaves is built from. One implementation per form, chosen by the publisher's type, so no
//! publish position ever asks which form it got.

use std::mem;

use bytes::BytesMut;
use serde::Serialize;

use super::{Outgoing, OutgoingName, Payload, Serialized, WireBytes};
use crate::codec::{Codec, CodecError};
use crate::{HeaderMap, Lend, OutgoingMessage, PayloadForm, Take};

impl PayloadForm for Lend {
    type Form<'a> = &'a [u8];

    #[inline]
    fn encoded<'b, C, T>(
        codec: &C,
        value: &T,
        buf: &'b mut BytesMut,
    ) -> Result<&'b [u8], CodecError>
    where
        C: Codec,
        T: Serialize,
    {
        buf.clear();
        codec.encode_into(value, buf)?;
        Ok(&buf[..])
    }

    #[inline]
    fn serialized<'v, T>(value: &'v T, buf: &'v mut BytesMut) -> Result<&'v [u8], T::Error>
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
}

impl PayloadForm for Take {
    type Form<'a> = BytesMut;

    #[inline]
    fn encoded<'b, C, T>(
        codec: &C,
        value: &T,
        _buf: &'b mut BytesMut,
    ) -> Result<BytesMut, CodecError>
    where
        C: Codec,
        T: Serialize,
    {
        // The transport keeps what it is handed, so the codec's own buffer travels to it and the
        // caller's scratch is left alone.
        codec.encode(value)
    }

    #[inline]
    fn serialized<'v, T>(value: &'v T, buf: &'v mut BytesMut) -> Result<BytesMut, T::Error>
    where
        T: Serialized,
    {
        buf.clear();
        match value.wire_bytes(buf)? {
            // The value wrote into this buffer and nothing else holds it, so the transport is
            // handed it whole. A dispatch loop lending its scratch here gets an empty one back
            // and grows a new buffer for the next message, which is what a taking transport
            // costs.
            WireBytes::InBuffer => Ok(mem::take(buf)),
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
}
