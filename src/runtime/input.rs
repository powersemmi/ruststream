//! The input axis: how one delivery materializes into the handler's message parameter.
//!
//! A definition names its input as a marker type: [`Decoded<T>`] decodes the payload with the
//! scope codec and lends the handler `&T`, [`RawBytes`] lends the payload itself as `&[u8]` -
//! no codec, no copy. The adapter owns the decode product for the duration of the call
//! ([`InputKind::Owned`], held on its stack) and the handler borrows a reference to
//! [`InputKind::Target`], so no allocation, copying, or boxing appears on the delivery
//! path: the raw form borrows straight out of the broker's buffer on every broker. Adding an
//! input kind is one pair of impls, not a new definition form.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;

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
/// this for every `C` without touching the payload.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be decoded with the codec `{DecodeCodec}`",
    note = "a typed input needs `serde::de::DeserializeOwned`; a raw `&[u8]` input decodes with any codec"
)]
pub trait DecodeWith<DecodeCodec>: InputKind {
    /// Decodes one delivery's payload.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the payload does not decode; the adapter applies the
    /// definition's decode failure policy.
    fn decode(codec: &DecodeCodec, payload: &[u8]) -> Result<Self::Owned, CodecError>;
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
    fn decode(codec: &DecodeCodec, payload: &[u8]) -> Result<T, CodecError> {
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
    fn decode(_codec: &DecodeCodec, _payload: &[u8]) -> Result<(), CodecError> {
        Ok(())
    }
}
