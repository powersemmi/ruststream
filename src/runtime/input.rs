//! The input axis: how one delivery materializes into the handler's message parameter.
//!
//! A definition names its input as a marker type: [`Decoded<T>`] decodes the payload with the
//! scope codec and lends the handler `&T`, [`RawBytes`] lends the payload itself as `&[u8]` -
//! no codec, no copy - and [`DecodedPair<H, P>`] decodes the payload and the delivery's typed
//! header contract together, lending a [`Message<H, P>`](crate::runtime::Message) pair. The
//! adapter owns the decode product for the duration of the call ([`InputKind::Owned`], held on
//! its stack) and the handler borrows a reference to [`InputKind::Target`], so no allocation,
//! copying, or boxing appears on the delivery path: the raw form borrows straight out of the
//! broker's buffer on every broker.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::HeaderMap;
use crate::codec::{Codec, CodecError};

/// One kind of handler input: the owned decode product and the borrowed view lent to the
/// handler.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler input kind",
    note = "the `#[subscriber]` macro selects `Decoded<T>` for a typed `&T` parameter and `RawBytes` for a raw `&[u8]` one"
)]
pub trait InputKind: Send + Sync + 'static {
    /// The owned decode product, held by the adapter across the call.
    type Owned: Send + Sync;

    /// What the handler borrows: `&T` for a decoded input, `[u8]` behind the reference for a
    /// raw one.
    type Target: ?Sized + Sync;

    /// Lends the handler its view of the decode product and the delivery payload.
    fn view<'a>(owned: &'a Self::Owned, payload: &'a [u8]) -> &'a Self::Target;

    /// The label `AsyncAPI` metadata uses for this input.
    fn input_label() -> &'static str;
}

/// An [`InputKind`] that knows how to decode itself with the codec `C`.
///
/// Split from [`InputKind`] so the view machinery stays codec-free: [`RawBytes`] implements
/// this for every `C` without touching the payload. The delivery's headers travel next to the
/// payload so a pair input ([`DecodedPair`]) materializes its typed header contract in the same
/// stage, under the same decode failure policy.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be decoded with the codec `{DecodeCodec}`",
    note = "a typed input needs `serde::de::DeserializeOwned`; a raw `&[u8]` input decodes with any codec"
)]
pub trait DecodeWith<DecodeCodec>: InputKind {
    /// Decodes one delivery's payload (and, for a pair input, its headers).
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the payload or the header contract does not decode; the
    /// adapter applies the definition's decode failure policy.
    fn decode(
        codec: &DecodeCodec,
        payload: &[u8],
        headers: &HeaderMap,
    ) -> Result<Self::Owned, CodecError>;
}

/// The typed input kind: the payload decodes into an owned `T`, the handler borrows `&T`.
pub struct Decoded<T>(PhantomData<T>);

impl<T> std::fmt::Debug for Decoded<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoded").finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> InputKind for Decoded<T> {
    type Owned = T;
    type Target = T;

    fn view<'a>(owned: &'a T, _payload: &'a [u8]) -> &'a T {
        owned
    }

    fn input_label() -> &'static str {
        std::any::type_name::<T>()
    }
}

impl<DecodeCodec: Codec, T: DeserializeOwned + Send + Sync + 'static> DecodeWith<DecodeCodec>
    for Decoded<T>
{
    fn decode(codec: &DecodeCodec, payload: &[u8], _headers: &HeaderMap) -> Result<T, CodecError> {
        codec.decode(payload)
    }
}

/// The raw input kind: nothing decodes, the handler borrows the payload bytes as delivered.
#[derive(Debug, Clone, Copy)]
pub struct RawBytes;

impl InputKind for RawBytes {
    type Owned = ();
    type Target = [u8];

    fn view<'a>(_owned: &'a (), payload: &'a [u8]) -> &'a [u8] {
        payload
    }

    fn input_label() -> &'static str {
        "bytes"
    }
}

impl<DecodeCodec> DecodeWith<DecodeCodec> for RawBytes {
    fn decode(
        _codec: &DecodeCodec,
        _payload: &[u8],
        _headers: &HeaderMap,
    ) -> Result<(), CodecError> {
        Ok(())
    }
}

/// The pair input kind: the payload decodes into `P` and the delivery's headers into the
/// contract `H`, both under the same decode failure policy; the handler borrows the
/// [`Message<H, P>`](crate::runtime::Message) pair.
pub struct DecodedPair<H, P>(PhantomData<(H, P)>);

impl<H, P> std::fmt::Debug for DecodedPair<H, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedPair").finish_non_exhaustive()
    }
}

impl<H, P> InputKind for DecodedPair<H, P>
where
    H: Send + Sync + 'static,
    P: Send + Sync + 'static,
{
    type Owned = crate::runtime::Message<H, P>;
    type Target = crate::runtime::Message<H, P>;

    fn view<'a>(owned: &'a Self::Owned, _payload: &'a [u8]) -> &'a Self::Target {
        owned
    }

    fn input_label() -> &'static str {
        std::any::type_name::<P>()
    }
}

impl<DecodeCodec: Codec, H, P> DecodeWith<DecodeCodec> for DecodedPair<H, P>
where
    H: DeserializeOwned + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
{
    fn decode(
        codec: &DecodeCodec,
        payload: &[u8],
        headers: &HeaderMap,
    ) -> Result<Self::Owned, CodecError> {
        let contract: H = headers
            .to_typed()
            .map_err(|err| CodecError::Decode(Box::from(err.to_string())))?;
        let body: P = codec.decode(payload)?;
        Ok(crate::runtime::Message::new(contract, body))
    }
}
