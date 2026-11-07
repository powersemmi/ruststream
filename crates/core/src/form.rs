//! What the runtime does with a payload on each side of a publisher's declaration.
//!
//! [`PayloadForm`] says what a transport is handed; these are its other half, the framework's
//! own: where the encode writes, how the pipeline carries the result, and what the message that
//! leaves is built from. One implementation per form, chosen by the publisher's type, so no
//! publish position ever asks which form it got.

use core::marker::PhantomData;
#[cfg(not(feature = "alloc"))]
use core::mem;

#[cfg(not(feature = "alloc"))]
use crate::PAYLOAD_CAPACITY;
use crate::codec::CodecError;
use crate::outgoing::{Outgoing, OutgoingName, Payload};
#[cfg(not(feature = "alloc"))]
use crate::publisher::Capped;
use crate::publisher::{KeptPayload, Unbounded};
use crate::wire::{Serialized, WireBytes, WireFailure, value_failed};
use crate::{
    EncodeBuf, Headers, Lend, OutgoingMessage, PayloadBuf, PayloadEncode, PayloadForm, Take,
};

/// The destination as the message that leaves reads it, kept in the spent stage's own slot
/// where the stage owns it.
///
/// A name the mount site borrowed - the literal every `#[subscriber(.., reply("dest"))]`
/// carries - is already valid for the publish and is kept nowhere.
#[inline]
fn kept<'a>(name: OutgoingName<'a>, slot: &'a mut Option<OutgoingName<'static>>) -> &'a str {
    match name {
        OutgoingName::Borrowed(name) => name,
        OutgoingName::Owned(name) => slot.insert(OutgoingName::Owned(name)).as_str(),
        #[cfg(feature = "alloc")]
        OutgoingName::Shared(name) => slot.insert(OutgoingName::Shared(name)).as_str(),
    }
}

// The map of a message the stage is done with, kept where the message that leaves can borrow it.
// It is written over with the entries and bytes it holds, never moved as a whole room.
#[cfg(not(feature = "alloc"))]
#[inline]
fn kept_map<'a>(slot: &'a mut Headers, headers: &Headers) -> &'a Headers {
    slot.clone_from(headers);
    slot
}

// What a buffer becomes on its way into `KeptPayload` and back: `Bytes` with `alloc` (a freeze
// and a unique thaw), the buffer itself without.
#[cfg(feature = "alloc")]
fn frozen(buf: PayloadBuf) -> bytes::Bytes {
    buf.freeze()
}
#[cfg(not(feature = "alloc"))]
const fn frozen(buf: PayloadBuf) -> PayloadBuf {
    buf
}

#[cfg(feature = "alloc")]
fn thawed(bytes: bytes::Bytes) -> PayloadBuf {
    // The buffer is the one the transport handed back, frozen and still unique, so it turns into
    // a buffer again without a copy.
    bytes
        .try_into_mut()
        .unwrap_or_else(|shared| PayloadBuf::from(&*shared))
}
#[cfg(not(feature = "alloc"))]
const fn thawed(buf: PayloadBuf) -> PayloadBuf {
    buf
}

#[cfg(feature = "alloc")]
fn owned_of(buffer: &mut PayloadBuf) -> bytes::Bytes {
    buffer.split().freeze()
}
#[cfg(not(feature = "alloc"))]
fn owned_of(buffer: &mut PayloadBuf) -> PayloadBuf {
    mem::take(buffer)
}

#[cfg(feature = "alloc")]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
fn copied<Error>(bytes: &[u8]) -> Result<PayloadBuf, WireFailure<Error>> {
    Ok(PayloadBuf::from(bytes))
}
#[cfg(not(feature = "alloc"))]
fn copied<Error>(bytes: &[u8]) -> Result<PayloadBuf, WireFailure<Error>> {
    PayloadBuf::try_from(bytes).map_err(WireFailure::Full)
}

// The lent bytes of a message that leaves on a taking transport, copied into the buffer it keeps.
// A fixed buffer holds what a payload may be (`PAYLOAD_CAPACITY`), and a lent payload past it is
// cut at the room: a stage that wrote one that long was refused by the encode before.
#[cfg(feature = "alloc")]
fn copied_lent(bytes: &[u8]) -> PayloadBuf {
    PayloadBuf::from(bytes)
}
#[cfg(not(feature = "alloc"))]
fn copied_lent(bytes: &[u8]) -> PayloadBuf {
    PayloadBuf::try_from(bytes).unwrap_or_default()
}

#[cfg(feature = "alloc")]
fn taken_payload(out: &mut Outgoing<'_>) -> PayloadBuf {
    out.take_payload()
}
#[cfg(not(feature = "alloc"))]
fn taken_payload(out: &mut Outgoing<'_>) -> PayloadBuf {
    out.take_payload().unwrap_or_default()
}

impl PayloadForm for Lend {
    const LENDS: bool = true;
    type Room = Unbounded;

    type Form<'a> = &'a [u8];

    // The codec writes into the publish path's own buffer, so the path carries it.
    type Encode<'a> = &'a mut EncodeBuf;

    // The destination where the stage owns it, and the buffer where a stage above the leaf
    // wrote one: the transport reads both, so both outlive the send.
    #[cfg(feature = "alloc")]
    type Spent = (Option<OutgoingName<'static>>, Option<PayloadBuf>);
    // The header map as well where there is no heap: the message borrows it.
    #[cfg(not(feature = "alloc"))]
    type Spent = (Option<OutgoingName<'static>>, Option<PayloadBuf>, Headers);

    #[inline]
    fn encode_slot(buf: &mut EncodeBuf) -> &mut EncodeBuf {
        buf
    }

    // The slot is itself the borrow of the loop's buffer, so lending it again is a borrow of a
    // borrow; there is no shape of this that is not `&mut &mut`.
    #[allow(clippy::mut_mut)]
    #[inline]
    fn encode_again<'s, 'a: 's>(slot: &'s mut &'a mut EncodeBuf) -> &'s mut EncodeBuf {
        slot
    }

    #[inline]
    fn encoded<'b, E>(encode: &E, buf: Self::Encode<'b>) -> Result<Self::Form<'b>, CodecError>
    where
        E: PayloadEncode,
    {
        buf.clear();
        encode.encode_into(buf)?;
        Ok(&**buf)
    }

    #[inline]
    fn serialized<'v, T>(
        value: &'v T,
        buf: Self::Encode<'v>,
    ) -> Result<Self::Form<'v>, WireFailure<T::Error>>
    where
        T: Serialized,
    {
        buf.clear();
        // Both answers are lent as they are: the transport reads them and keeps neither.
        Ok(value.wire_bytes(buf).map_err(value_failed)?.of(buf))
    }

    #[inline]
    fn kept(payload: &[u8]) -> KeptPayload {
        KeptPayload::Lent {
            addr: payload.as_ptr() as usize,
            len: payload.len(),
        }
    }

    #[inline]
    fn restored(_kept: KeptPayload, _out: &mut Outgoing<'_>) {}

    #[inline]
    fn rebuilt<'a>(name: OutgoingName<'a>, payload: &'a [u8], headers: Headers) -> Outgoing<'a> {
        Outgoing::rebuilding(name, Payload::Lent(payload), headers)
    }

    #[inline]
    #[cfg(feature = "alloc")]
    fn leaving<'a>(out: &'a mut Outgoing<'_>) -> OutgoingMessage<'a, &'a [u8]> {
        let headers = out.take_headers();
        // The mutable borrow ends here; what is left is read, and read for as long as the
        // message that leaves lives.
        let out: &'a Outgoing<'_> = out;
        OutgoingMessage::assembled(out.name(), out.payload(), headers)
    }

    // The map stays where the stages wrote it, and the message borrows it there with the rest.
    #[inline]
    #[cfg(not(feature = "alloc"))]
    fn leaving<'a>(out: &'a mut Outgoing<'_>) -> OutgoingMessage<'a, &'a [u8]> {
        let out: &'a Outgoing<'_> = out;
        OutgoingMessage::assembled(out.name(), out.payload(), out.headers())
    }

    #[inline]
    #[cfg(not(feature = "alloc"))]
    fn shortened<'short, 'long: 'short>(payload: &'long [u8]) -> &'short [u8] {
        payload
    }

    #[inline]
    fn handed_on<'a>(
        out: Outgoing<'a>,
        spent: &'a mut Self::Spent,
    ) -> OutgoingMessage<'a, &'a [u8]> {
        let (name, payload, headers) = out.into_parts();
        #[cfg(feature = "alloc")]
        let (kept_name, kept_buffer) = spent;
        #[cfg(not(feature = "alloc"))]
        let (kept_name, kept_buffer, kept_headers) = spent;
        let bytes = match payload {
            Payload::Lent(bytes) => bytes,
            Payload::Consumed => &[],
            // A stage above wrote a buffer of its own; the transport only reads it, so it stays
            // with the caller until the publish is over.
            Payload::Owned(buf) => &**kept_buffer.insert(buf),
        };
        #[cfg(not(feature = "alloc"))]
        let headers = kept_map(kept_headers, &headers);
        OutgoingMessage::assembled(kept(name, kept_name), bytes, headers)
    }

    #[inline]
    fn reclaimed(spent: &mut Self::Spent, kept: &mut KeptPayload) {
        if let (KeptPayload::Lent { addr, len }, Some(buffer)) = (&*kept, &mut spent.1)
            && buffer.as_ptr() as usize == *addr
            && buffer.len() == *len
        {
            *kept = KeptPayload::Owned(owned_of(buffer));
        }
    }
}

impl PayloadForm for Take {
    const LENDS: bool = false;
    #[cfg(feature = "alloc")]
    type Room = Unbounded;
    #[cfg(not(feature = "alloc"))]
    type Room = Capped<PAYLOAD_CAPACITY>;

    type Form<'a> = PayloadBuf;

    // The codec produces the buffer the transport keeps, so there is nothing to write into: a
    // publish on this form carries the scratch's borrow and none of its bytes, which is what
    // keeps it out of every future on the way.
    type Encode<'a> = PhantomData<&'a mut EncodeBuf>;

    // The destination alone, and only where the stage owns it: the payload and the map move
    // into the message the transport keeps.
    #[cfg(feature = "alloc")]
    type Spent = Option<OutgoingName<'static>>;
    // The header map as well where there is no heap: the message borrows it.
    #[cfg(not(feature = "alloc"))]
    type Spent = (Option<OutgoingName<'static>>, Headers);

    #[inline]
    fn encode_slot(_buf: &mut EncodeBuf) -> PhantomData<&mut EncodeBuf> {
        PhantomData
    }

    #[inline]
    fn encode_again<'s, 'a: 's>(
        _slot: &'s mut PhantomData<&'a mut EncodeBuf>,
    ) -> PhantomData<&'s mut EncodeBuf> {
        PhantomData
    }

    #[inline]
    fn encoded<'b, E>(encode: &E, _buf: Self::Encode<'b>) -> Result<Self::Form<'b>, CodecError>
    where
        E: PayloadEncode,
    {
        // The transport keeps what it is handed, so the codec's own buffer travels to it and the
        // caller's scratch is never touched.
        encode.encode()
    }

    #[inline]
    fn serialized<'v, T>(
        value: &'v T,
        _buf: Self::Encode<'v>,
    ) -> Result<Self::Form<'v>, WireFailure<T::Error>>
    where
        T: Serialized,
    {
        // A value that writes its bytes writes them into the buffer the transport will keep,
        // which is this one; a value that holds them is copied into it.
        let mut buf = PayloadBuf::new();
        match value.wire_bytes(&mut buf).map_err(value_failed)? {
            // The value wrote into this buffer and nothing else holds it, so the transport is
            // handed it whole.
            WireBytes::InBuffer => Ok(buf),
            // The bytes belong to the value, which outlives neither the publish nor the
            // transport's claim on them, so this is the one copy the form asks for.
            WireBytes::Own(bytes) => copied(bytes),
        }
    }

    #[inline]
    fn kept(payload: PayloadBuf) -> KeptPayload {
        KeptPayload::Owned(frozen(payload))
    }

    #[inline]
    fn restored(kept: KeptPayload, out: &mut Outgoing<'_>) {
        if let KeptPayload::Owned(bytes) = kept {
            // The buffer is the one the transport handed back, frozen and still unique, so it
            // turns into a buffer again without a copy.
            out.set_payload(thawed(bytes));
        }
    }

    #[inline]
    fn rebuilt(name: OutgoingName<'_>, payload: PayloadBuf, headers: Headers) -> Outgoing<'_> {
        Outgoing::rebuilding(name, Payload::Owned(payload), headers)
    }

    #[inline]
    #[cfg(feature = "alloc")]
    fn leaving<'a>(out: &'a mut Outgoing<'_>) -> OutgoingMessage<'a, PayloadBuf> {
        let payload = taken_payload(out);
        let headers = out.take_headers();
        OutgoingMessage::assembled(out.name(), payload, headers)
    }

    // The buffer is the transport's to keep; the map stays where the stages wrote it, and the
    // message borrows it there.
    #[inline]
    #[cfg(not(feature = "alloc"))]
    fn leaving<'a>(out: &'a mut Outgoing<'_>) -> OutgoingMessage<'a, PayloadBuf> {
        let payload = taken_payload(out);
        let out: &'a Outgoing<'_> = out;
        OutgoingMessage::assembled(out.name(), payload, out.headers())
    }

    #[inline]
    #[cfg(not(feature = "alloc"))]
    fn shortened<'short, 'long: 'short>(payload: Self::Form<'long>) -> Self::Form<'short> {
        payload
    }

    #[inline]
    fn handed_on<'a>(
        out: Outgoing<'a>,
        spent: &'a mut Self::Spent,
    ) -> OutgoingMessage<'a, PayloadBuf> {
        // The payload and the map move into the message the transport keeps, so the stage is
        // left holding the destination and nothing else.
        let (name, payload, headers) = out.into_parts();
        let payload = match payload {
            Payload::Lent(bytes) => copied_lent(bytes),
            Payload::Owned(buf) => buf,
            Payload::Consumed => PayloadBuf::new(),
        };
        #[cfg(feature = "alloc")]
        let (kept_name, headers) = (spent, headers);
        #[cfg(not(feature = "alloc"))]
        let (kept_name, headers) = {
            let (kept_name, kept_headers) = spent;
            (kept_name, kept_map(kept_headers, &headers))
        };
        OutgoingMessage::assembled(kept(name, kept_name), payload, headers)
    }

    #[inline]
    fn reclaimed(_spent: &mut Self::Spent, _kept: &mut KeptPayload) {}
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use crate::outgoing::Outgoing;
    use crate::{Headers, Lend, PayloadBuf, PayloadForm, Take};

    #[test]
    fn the_terminal_hands_the_message_over_in_the_publisher_s_form() {
        // What a taking broker is handed: the buffer the pipeline wrote, as it wrote it.
        let mut produced = Outgoing::new("t", PayloadBuf::from(&b"body"[..]));
        let wrote_at = produced.payload().as_ptr() as usize;
        let taken = <Take as PayloadForm>::leaving(&mut produced);
        assert_eq!(
            taken.payload().as_ptr() as usize,
            wrote_at,
            "the buffer moves into the message rather than being copied into it",
        );

        // A rebuilt message carries that buffer through untouched, so a slot's transforms do not
        // cost the broker its hand-over.
        let mut rebuilt = <Take as PayloadForm>::rebuilt(
            "t".into(),
            PayloadBuf::from(&b"body"[..]),
            Headers::new(),
        );
        let carried_at = rebuilt.payload().as_ptr() as usize;
        assert_eq!(
            <Take as PayloadForm>::leaving(&mut rebuilt)
                .payload()
                .as_ptr() as usize,
            carried_at,
        );

        // What a reading broker is handed: the bytes where the pipeline left them, whether it was
        // lent them or wrote them itself.
        let body = b"body".to_vec();
        let mut lent = <Lend as PayloadForm>::rebuilt("t".into(), &body, Headers::new());
        assert_eq!(
            <Lend as PayloadForm>::leaving(&mut lent).payload().as_ptr() as usize,
            body.as_ptr() as usize,
            "bytes the pipeline was lent are lent on, with nothing copied on the way",
        );
    }

    /// A stage that hands a message on to a taking transport leaves it holding the destination
    /// alone: lent bytes are copied, owned ones move, and a consumed message leaves empty.
    #[test]
    fn a_taking_transport_is_handed_the_bytes_in_whatever_form_the_stage_held_them() {
        let mut spent = None;
        let lent_bytes = b"lent".to_vec();
        let lent = <Lend as PayloadForm>::rebuilt("t".into(), &lent_bytes, Headers::new());
        let handed = <Take as PayloadForm>::handed_on(lent, &mut spent);
        assert_eq!(handed.payload(), b"lent");

        let mut spent = None;
        let owned = Outgoing::new("t", PayloadBuf::from(&b"owned"[..]));
        let handed = <Take as PayloadForm>::handed_on(owned, &mut spent);
        assert_eq!(handed.payload(), b"owned");

        let mut spent = None;
        let mut consumed = Outgoing::new("t", b"gone".as_slice());
        consumed.consume();
        let handed = <Take as PayloadForm>::handed_on(consumed, &mut spent);
        assert_eq!(handed.payload(), b"");
    }
}

// Without a heap the message that leaves borrows the map: from the pipeline's message while the
// stage holds it, from what the stage keeps once it spent it.
#[cfg(all(test, not(feature = "alloc")))]
mod fixed_tests {
    use core::ptr;

    use crate::{Headers, HeadersFull, Lend, PayloadBuf, PayloadForm, Take};

    fn traced() -> Result<Headers, HeadersFull> {
        let mut headers = Headers::new();
        headers.insert("trace", "t1")?;
        Ok(headers)
    }

    #[test]
    fn the_message_that_leaves_reads_the_map_where_the_stages_wrote_it() -> Result<(), HeadersFull>
    {
        let mut lent = <Lend as PayloadForm>::rebuilt("t".into(), b"body", traced()?);
        let wrote_at: *const Headers = lent.headers();
        let leaving = <Lend as PayloadForm>::leaving(&mut lent);
        assert!(ptr::eq(leaving.headers(), wrote_at));
        assert_eq!(leaving.headers().get_str("trace"), Some("t1"));

        let mut taken = <Take as PayloadForm>::rebuilt(
            "t".into(),
            PayloadBuf::try_from(b"body".as_slice()).map_err(|_| HeadersFull)?,
            traced()?,
        );
        let wrote_at: *const Headers = taken.headers();
        let leaving = <Take as PayloadForm>::leaving(&mut taken);
        assert!(ptr::eq(leaving.headers(), wrote_at));
        assert_eq!(leaving.payload(), b"body");
        Ok(())
    }

    #[test]
    fn a_spent_message_lends_the_map_the_stage_keeps() -> Result<(), HeadersFull> {
        let mut spent = <Lend as PayloadForm>::Spent::default();
        let out = <Lend as PayloadForm>::rebuilt("t".into(), b"body", traced()?);
        let handed = <Lend as PayloadForm>::handed_on(out, &mut spent);
        assert_eq!(handed.headers().get_str("trace"), Some("t1"));
        let lent_at: *const Headers = handed.headers();
        assert!(ptr::eq(lent_at, &spent.2));

        let mut spent = <Take as PayloadForm>::Spent::default();
        let mut out = <Lend as PayloadForm>::rebuilt("t".into(), b"body", Headers::new());
        out.headers_mut().insert("trace", "t2")?;
        let handed = <Take as PayloadForm>::handed_on(out, &mut spent);
        assert_eq!(handed.headers().get_str("trace"), Some("t2"));
        let lent_at: *const Headers = handed.headers();
        assert!(ptr::eq(lent_at, &spent.1));
        Ok(())
    }
}
