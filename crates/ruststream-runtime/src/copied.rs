//! The owned copy the body keeps of a delivery's payload and of a destination name.
//!
//! A failed message, a retry copy and a kept delivery outlive the buffer they were read from. With
//! `alloc` the copy is a shared `Bytes` and a `String`. Without it the copy lives in a buffer of
//! fixed size, so a payload larger than [`PAYLOAD_CAPACITY`](ruststream_core::PAYLOAD_CAPACITY)
//! or a name longer than [`NAME_CAPACITY`] bytes is not kept: [`copy_payload`] and [`copy_name`]
//! answer `None`, and the caller keeps the message without it.

#[cfg(ruststream_alloc)]
use alloc::borrow::{Cow, ToOwned};
#[cfg(ruststream_alloc)]
use alloc::string::String;
#[cfg(ruststream_alloc)]
use bytes::Bytes;

#[cfg(not(ruststream_alloc))]
use ruststream_core::PayloadBuf;
use ruststream_core::PayloadForm;

use crate::HeaderSet;
use ruststream_core::NameText;

/// The room of a kept destination name in a build without `alloc`, in bytes: the room a refused
/// publish gives the destination it returns.
#[cfg(not(ruststream_alloc))]
pub(crate) const NAME_CAPACITY: usize = 64;

/// An owned copy of a payload: `Bytes` with `alloc`, a fixed buffer without.
#[cfg(ruststream_alloc)]
pub(crate) type CopiedPayload = Bytes;
/// An owned copy of a payload: `Bytes` with `alloc`, a fixed buffer without.
#[cfg(not(ruststream_alloc))]
pub(crate) type CopiedPayload = PayloadBuf;

/// An owned copy of a destination name: a `String` with `alloc`, a fixed string without.
#[cfg(ruststream_alloc)]
pub(crate) type CopiedName = String;
/// An owned copy of a destination name: a `String` with `alloc`, a fixed string without.
#[cfg(not(ruststream_alloc))]
pub(crate) type CopiedName = heapless::String<NAME_CAPACITY>;

/// A copy address a subscription keeps for its retry copies: a `String` with `alloc`; without it
/// the literal the address was made from, held whole, because an address is never built at run
/// time there.
#[cfg(ruststream_alloc)]
pub(crate) type KeptName = String;
/// A copy address a subscription keeps for its retry copies: a `String` with `alloc`; without it
/// the literal the address was made from, held whole, because an address is never built at run
/// time there.
#[cfg(not(ruststream_alloc))]
pub(crate) type KeptName = NameText;

/// A copy of a delivery that does not fit the fixed room it would be built in. It is the cause a
/// path reports when it cannot make the copy: the broker moves the delivery instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the copy does not fit the fixed room")]
pub(crate) struct NoRoom;

/// A destination name a call site supplies: borrowed, or owned where there is a heap to own it.
#[cfg(ruststream_alloc)]
pub(crate) type NameCow<'name> = Cow<'name, str>;
/// A destination name a call site supplies: borrowed, or owned where there is a heap to own it.
#[cfg(not(ruststream_alloc))]
pub(crate) type NameCow<'name> = &'name str;

/// A supplied name's text, borrowed.
#[cfg(ruststream_alloc)]
pub(crate) fn supplied_text<'name>(name: &'name NameCow<'_>) -> &'name str {
    name
}

/// A supplied name's text, borrowed.
#[cfg(not(ruststream_alloc))]
pub(crate) const fn supplied_text<'name>(name: &NameCow<'name>) -> &'name str {
    name
}

/// Copies `payload`; without `alloc`, `None` when it does not fit the fixed buffer.
#[cfg(ruststream_alloc)]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
pub(crate) fn copy_payload(payload: &[u8]) -> Option<CopiedPayload> {
    Some(Bytes::copy_from_slice(payload))
}

/// Copies `payload`; without `alloc`, `None` when it does not fit the fixed buffer.
#[cfg(not(ruststream_alloc))]
pub(crate) fn copy_payload(payload: &[u8]) -> Option<CopiedPayload> {
    CopiedPayload::try_from(payload).ok()
}

/// Copies `name`; without `alloc`, `None` when it does not fit the fixed string.
#[cfg(ruststream_alloc)]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
pub(crate) fn copy_name(name: &str) -> Option<CopiedName> {
    Some(String::from(name))
}

/// Copies `name`; without `alloc`, `None` when it does not fit the fixed string.
#[cfg(not(ruststream_alloc))]
pub(crate) fn copy_name(name: &str) -> Option<CopiedName> {
    CopiedName::try_from(name).ok()
}

/// A name kept for a diagnostic: whole with `alloc`; without it cut, on a character, to the room.
/// Never used to route: a cut name would reach a different destination.
#[cfg(ruststream_alloc)]
pub(crate) fn diagnostic_name(name: &str) -> CopiedName {
    String::from(name)
}

/// See the `alloc` form.
/// A name's text, borrowed.
#[cfg(ruststream_alloc)]
pub(crate) fn name_text(name: &NameText) -> &str {
    name.as_ref()
}

/// A name's text, borrowed.
#[cfg(not(ruststream_alloc))]
pub(crate) const fn name_text(name: &NameText) -> &str {
    name
}

#[cfg(not(ruststream_alloc))]
pub(crate) fn diagnostic_name(name: &str) -> CopiedName {
    let mut end = name.len().min(NAME_CAPACITY);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    let mut cut = CopiedName::new();
    // Cannot fail: `end` is within the room.
    if let Some(head) = name.get(..end) {
        let _ = cut.push_str(head);
    }
    cut
}

/// The delivery's headers laid over the publisher's constants: the delivery's value wins where
/// both name a header. Without `alloc`, `None` when the result does not fit the fixed map.
#[cfg(ruststream_alloc)]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
pub(crate) fn lay_over(base: &HeaderSet, delivered: &HeaderSet) -> Option<HeaderSet> {
    let mut headers = base.clone();
    for (key, value) in delivered.iter() {
        headers.insert(key.to_owned(), value.to_owned());
    }
    Some(headers)
}

/// The delivery's headers laid over the publisher's constants: the delivery's value wins where
/// both name a header. Without `alloc`, `None` when the result does not fit the fixed map.
#[cfg(not(ruststream_alloc))]
pub(crate) fn lay_over(base: &HeaderSet, delivered: &HeaderSet) -> Option<HeaderSet> {
    let mut headers = base.clone();
    for (key, value) in delivered.iter() {
        headers.insert(key, value).ok()?;
    }
    Some(headers)
}

/// A lent payload in the form publisher `P` is handed: a copy where the transport keeps the
/// bytes. Without `alloc`, `None` when it does not fit the fixed buffer.
#[cfg(ruststream_alloc)]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
pub(crate) fn form_of<P: PayloadForm>(payload: &[u8]) -> Option<P::Form<'_>> {
    Some(P::Form::from(payload))
}

/// A lent payload in the form publisher `P` is handed: a copy where the transport keeps the
/// bytes. Without `alloc`, `None` when it does not fit the fixed buffer.
#[cfg(not(ruststream_alloc))]
pub(crate) fn form_of<P: PayloadForm>(payload: &[u8]) -> Option<P::Form<'_>> {
    P::Form::try_from(payload).ok()
}

/// An empty payload copy.
#[cfg(ruststream_alloc)]
pub(crate) fn empty_payload() -> CopiedPayload {
    Bytes::new()
}

/// An empty payload copy.
#[cfg(not(ruststream_alloc))]
pub(crate) fn empty_payload() -> CopiedPayload {
    CopiedPayload::new()
}

/// Copies `payload`, or an empty copy where it does not fit: what a failure path keeps when it
/// has room for nothing more.
pub(crate) fn copy_or_empty(payload: &[u8]) -> CopiedPayload {
    copy_payload(payload).unwrap_or_else(empty_payload)
}

/// Turns an encode buffer the body is done with into a kept copy, without a copy where the
/// build has a heap.
#[cfg(ruststream_alloc)]
pub(crate) fn freeze(buf: ruststream_core::PayloadBuf) -> CopiedPayload {
    buf.freeze()
}

/// Turns an encode buffer the body is done with into a kept copy.
#[cfg(not(ruststream_alloc))]
pub(crate) fn freeze(buf: PayloadBuf) -> CopiedPayload {
    buf
}

/// Moves what an encode buffer holds into a kept copy and leaves the buffer empty.
#[cfg(ruststream_alloc)]
pub(crate) fn take_frozen(buf: &mut ruststream_core::EncodeBuf) -> CopiedPayload {
    buf.split().freeze()
}

/// Moves what an encode buffer holds into a kept copy and leaves the buffer empty. A buffer
/// roomier than the copy's own (a mount asked for it with `encode_capacity`) can hold more than a
/// copy does: what does not fit is kept as nothing, as everywhere a copy has no room.
#[cfg(not(ruststream_alloc))]
pub(crate) fn take_frozen(buf: &mut ruststream_core::EncodeBuf) -> CopiedPayload {
    let kept = copy_or_empty(buf.as_slice());
    buf.clear();
    kept
}

#[cfg(test)]
#[cfg(not(ruststream_alloc))]
mod fixed_tests {
    use ruststream_core::PAYLOAD_CAPACITY;

    use super::*;

    #[test]
    fn a_payload_that_fits_is_copied() {
        let copy = copy_payload(b"body").expect("fits");
        assert_eq!(copy.as_slice(), b"body");
    }

    #[test]
    fn a_payload_past_the_room_is_not_copied() {
        assert!(copy_payload(&[0u8; PAYLOAD_CAPACITY + 1]).is_none());
    }

    #[test]
    fn a_payload_past_the_room_is_kept_as_nothing() {
        assert!(copy_or_empty(&[0u8; PAYLOAD_CAPACITY + 1]).is_empty());
        assert_eq!(copy_or_empty(b"body").as_slice(), b"body");
    }

    #[test]
    fn taking_a_buffer_leaves_it_empty() {
        let mut buf = PayloadBuf::new();
        buf.extend_from_slice(b"body").unwrap();
        let kept = take_frozen(&mut buf);
        assert_eq!(kept.as_slice(), b"body");
        assert!(buf.is_empty());
    }

    #[test]
    fn a_name_past_the_room_is_not_copied() {
        assert_eq!(copy_name("orders").expect("fits").as_str(), "orders");
        let long = [b'd'; NAME_CAPACITY + 1];
        assert!(copy_name(core::str::from_utf8(&long).unwrap()).is_none());
    }
}

#[cfg(test)]
#[cfg(ruststream_alloc)]
mod tests {
    use ruststream_core::{Lend, Take};

    use super::*;

    #[test]
    fn the_delivered_headers_lie_over_the_publishers_constants() {
        let mut base = HeaderSet::new();
        base.insert("source", "svc");
        base.insert("kind", "base");
        let mut delivered = HeaderSet::new();
        delivered.insert("kind", "delivered");
        delivered.insert("trace", "t1");

        let laid = lay_over(&base, &delivered).expect("a heap has room");

        assert_eq!(
            laid.get("source").map(AsRef::as_ref),
            Some(b"svc".as_slice())
        );
        assert_eq!(
            laid.get("kind").map(AsRef::as_ref),
            Some(b"delivered".as_slice())
        );
        assert_eq!(laid.get("trace").map(AsRef::as_ref), Some(b"t1".as_slice()));
    }

    #[test]
    fn a_lent_payload_becomes_the_form_a_publisher_takes() {
        let lent = form_of::<Lend>(b"body").expect("a heap has room");
        assert_eq!(lent, b"body".as_slice());
        let taken = form_of::<Take>(b"body").expect("a heap has room");
        assert_eq!(taken.as_ref(), b"body");
    }

    #[test]
    fn a_diagnostic_name_keeps_what_the_room_holds() {
        assert_eq!(diagnostic_name("orders"), "orders");
    }

    #[cfg(not(ruststream_alloc))]
    #[test]
    fn a_diagnostic_name_past_the_room_is_cut_on_a_character() {
        // 63 bytes, then a two-byte character that straddles the room.
        let name = alloc_free_name();
        let cut = diagnostic_name(name.as_str());
        assert_eq!(cut.len(), 63);
    }

    #[cfg(not(ruststream_alloc))]
    fn alloc_free_name() -> heapless::String<80> {
        let mut name = heapless::String::new();
        for _ in 0..63 {
            name.push('a').expect("fits");
        }
        name.push('\u{e9}').expect("fits");
        name
    }
}
