//! A value that carries its own bytes: [`Serialized`] writes or lends them, [`WireBytes`] says
//! which.

use core::error::Error as StdError;

#[cfg(not(feature = "alloc"))]
use core::fmt::{self, Display, Formatter};

use crate::EncodeBuf;

/// A value that owns its byte layout: `Serialize` means the framework's codec does the work,
/// `Serialized` means the type produces the bytes itself.
///
/// A `Serialized` value leaves the service byte-for-byte with no codec anywhere on the path,
/// selected purely by the type, on every typed surface: the `message(&value)` entry of a
/// publish builder (an [`Out`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.Out.html) slot, a transaction scope, any publisher),
/// and the reply position (the same `.reply()` chain and `reply("dest")` clause an encoded
/// reply uses).
///
/// The two shapes it covers are one method apart. A byte bag lends what it holds; a generated
/// message (Protobuf, Cap'n Proto, a hand-rolled frame) holds fields, and writes them into the
/// buffer the publish path hands it - so nothing is encoded twice and nothing intermediate is
/// allocated.
///
/// # Implementing by hand
///
/// `#[derive(Serialized)]` (under the `macros` feature) covers a newtype or single-field struct
/// over a byte buffer, and, with `#[wire(encode = ..)]`, any type whose format names an encode
/// function. Any other shape is the bytes plus the wire spellings that route the type onto the
/// serialized wire: [`MessageWire`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.MessageWire.html) for the typed publish entry, and
/// [`ReplyShape`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.ReplyShape.html) for the reply position.
///
/// ```
/// use std::convert::Infallible;
///
/// use ruststream::prelude::*;
///
/// struct Export(Vec<u8>);
///
/// impl Serialized for Export {
///     type Error = Infallible;
///
///     fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Infallible> {
///         Ok(WireBytes::Own(&self.0))
///     }
/// }
///
/// impl MessageWire for Export {
///     type Wire = SerializedWire;
/// }
///
/// impl ReplyShape for Export {
///     type Body = Self;
///     type Headers = ();
///     type Wire = SerializedReply;
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let export = Export(vec![7, 9]);
/// let mut buf = PayloadBuf::new();
/// assert!(matches!(export.wire_bytes(&mut buf)?, WireBytes::Own(bytes) if bytes == [7, 9]));
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
///
/// A type that computes its bytes writes them into the buffer and lends that:
///
/// ```
/// use std::convert::Infallible;
///
/// use ruststream::prelude::*;
///
/// struct Tick(u32);
///
/// impl Serialized for Tick {
///     type Error = Infallible;
///
///     fn wire_bytes(&self, buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Infallible> {
///         buf.extend_from_slice(&self.0.to_be_bytes());
///         Ok(WireBytes::InBuffer)
///     }
/// }
///
/// impl MessageWire for Tick {
///     type Wire = SerializedWire;
/// }
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let mut buf = PayloadBuf::new();
/// assert!(matches!(Tick(1).wire_bytes(&mut buf)?, WireBytes::InBuffer));
/// assert_eq!(&buf[..], &[0, 0, 0, 1]);
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
pub trait Serialized {
    /// The failure of producing the bytes; [`Infallible`](core::convert::Infallible) for a value
    /// that already holds them.
    // Tighter than `Deserialized::Error`'s `Display`: a publish failure travels as the `#[source]`
    // of `PublishError`, where a construction failure is only reported to the decode policy.
    type Error: StdError + Send + Sync + 'static;

    /// The bytes the value publishes, exactly as they leave on the wire: the ones it already
    /// holds, answered as [`WireBytes::Own`], or the ones it writes into `buf`, answered as
    /// [`WireBytes::InBuffer`].
    ///
    /// One method rather than a lend-or-compute pair, so a value cannot answer the two
    /// differently, and a value that carries its bytes never touches `buf` - which is why the
    /// publish path can hand it a buffer that has not allocated. Which answer it gives is what
    /// the path above it needs to know: a transport that keeps the payload is handed the buffer
    /// itself where the value wrote into it, and copies only where the bytes are the value's own.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the value's own encoder rejects it. A value that already
    /// holds its bytes cannot fail, and says so with `Infallible`.
    fn wire_bytes(&self, buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Self::Error>;
}

/// Where a [`Serialized`] value's bytes are: in the buffer it was handed, or in the value
/// itself.
///
/// It is the answer [`Serialized::wire_bytes`] gives, and the publish path needs it because the
/// two are not interchangeable underneath: a transport that keeps the payload takes the buffer
/// where the value wrote into it, and copies where the bytes belong to the value. A pointer
/// comparison would guess at the same fact; this states it.
///
/// # Examples
///
/// ```
/// use std::convert::Infallible;
///
/// use ruststream::prelude::*;
///
/// /// A frame that already holds its encoded bytes: it lends them, and the buffer it was handed
/// /// stays untouched.
/// struct Frame(Vec<u8>);
///
/// impl Serialized for Frame {
///     type Error = Infallible;
///
///     fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Infallible> {
///         Ok(WireBytes::Own(&self.0))
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum WireBytes<'a> {
    /// The value wrote its bytes into the buffer it was handed.
    InBuffer,
    /// The bytes are the value's own, lent for the length of the publish.
    Own(&'a [u8]),
}

impl<'a> WireBytes<'a> {
    /// The bytes themselves, given the buffer the value was handed.
    ///
    /// What a caller that only reads them uses; the publish path answers the two arms
    /// differently, because only one of them can be taken.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::PayloadBuf;
    /// use ruststream::runtime::Serialized;
    ///
    /// /// The size a value has on the wire, for a check against the broker's payload limit before
    /// /// the publish is attempted.
    /// fn wire_len<T: Serialized>(value: &T) -> Result<usize, T::Error> {
    ///     let mut buf = PayloadBuf::new();
    ///     Ok(value.wire_bytes(&mut buf)?.of(&buf).len())
    /// }
    /// ```
    #[must_use]
    pub fn of(self, buf: &'a EncodeBuf) -> &'a [u8] {
        match self {
            Self::InBuffer => buf,
            Self::Own(bytes) => bytes,
        }
    }
}

/// What serializing a value into the payload form of a publish reports: the value's own error
/// with `alloc`, where the buffer grows; with a fixed buffer, also that the bytes did not fit.
#[cfg(feature = "alloc")]
pub type WireFailure<Error> = Error;

/// What serializing a value into the payload form of a publish reports: the value's own error
/// with `alloc`, where the buffer grows; with a fixed buffer, also that the bytes did not fit.
#[cfg(not(feature = "alloc"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFailure<Error> {
    /// The value failed to produce its bytes.
    Value(Error),
    /// The bytes do not fit the fixed payload buffer.
    Full(crate::BufferFull),
}

#[cfg(not(feature = "alloc"))]
impl<Error: Display> Display for WireFailure<Error> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(error) => error.fmt(f),
            Self::Full(full) => full.fmt(f),
        }
    }
}

#[cfg(not(feature = "alloc"))]
impl<Error: StdError + 'static> StdError for WireFailure<Error> {}

/// The value's error as the failure of a serialization.
#[cfg(feature = "alloc")]
#[inline]
pub(crate) const fn value_failed<Error>(error: Error) -> WireFailure<Error> {
    error
}

/// The value's error as the failure of a serialization.
#[cfg(not(feature = "alloc"))]
#[inline]
pub(crate) const fn value_failed<Error>(error: Error) -> WireFailure<Error> {
    WireFailure::Value(error)
}
