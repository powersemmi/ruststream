//! The codecs of the contract.
//!
//! [`CodecError`] is what a heap codec reports when it fails. [`SliceCodec`] writes into a buffer
//! the caller owns; `Postcard` (feature `postcard`) and `JsonCore` (feature `json-core`)
//! implement it.

#[cfg(feature = "alloc")]
use alloc::boxed::Box;
#[cfg(feature = "alloc")]
use core::error::Error as StdError;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "json-core")]
mod json_core;
#[cfg(feature = "postcard")]
mod postcard;

#[cfg(feature = "json-core")]
pub use json_core::JsonCore;
#[cfg(feature = "postcard")]
pub use postcard::Postcard;

/// Errors returned by codec implementations.
#[cfg(feature = "alloc")]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The codec failed to encode a Rust value into bytes.
    #[error("encode failed: {0}")]
    Encode(#[source] Box<dyn StdError + Send + Sync>),

    /// The codec failed to decode bytes into a Rust value.
    #[error("decode failed: {0}")]
    Decode(#[source] Box<dyn StdError + Send + Sync>),
}

/// A serializer that writes a value into a buffer the caller owns and reads one out of a borrowed
/// frame, without a heap.
///
/// The codec of a device: the publish path hands it a fixed buffer and sends the bytes it wrote,
/// and the delivery path decodes the frame in place, so a `&str` or `&[u8]` field borrows from
/// the frame. Implementations are stateless unit structs; the methods are generic, so the codec
/// is a type fixed at compile time.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postcard")]
/// # mod demo {
/// use ruststream_core::codec::{Postcard, SliceCodec, SliceCodecError};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Reading<'a> {
///     sensor: &'a str,
///     celsius: f32,
/// }
///
/// /// A device's publish step: the reading is encoded into the transport's frame buffer.
/// fn publish_reading<Codec: SliceCodec>(
///     codec: &Codec,
///     reading: &Reading<'_>,
///     frame: &mut [u8; 64],
/// ) -> Result<usize, SliceCodecError> {
///     let written = codec.encode(reading, frame)?;
///     tracing::info!(content_type = Codec::CONTENT_TYPE, written, "frame ready");
///     Ok(written)
/// }
///
/// /// A handler's decode step: the sensor name borrows from the frame.
/// fn on_frame<Codec: SliceCodec>(codec: &Codec, frame: &[u8]) {
///     match codec.decode::<Reading<'_>>(frame) {
///         Ok(reading) => tracing::info!(reading.sensor, reading.celsius, "reading"),
///         Err(error) => tracing::warn!(%error, "frame dropped"),
///     }
/// }
///
/// fn round_trip() -> Result<(), SliceCodecError> {
///     let mut frame = [0; 64];
///     let reading = Reading { sensor: "boiler", celsius: 71.5 };
///     let written = publish_reading(&Postcard, &reading, &mut frame)?;
///     on_frame(&Postcard, &frame[..written]);
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub trait SliceCodec {
    /// The MIME type a message encoded by this codec is reported with.
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    /// Encodes `value` into the start of `buf` and returns how many bytes it wrote.
    ///
    /// # Errors
    ///
    /// Returns [`SliceCodecError::BufferFull`] when the value does not fit into `buf`, and
    /// [`SliceCodecError::Encode`] when the serializer rejects the value. The contents of `buf`
    /// are then unspecified.
    fn encode<T: Serialize + ?Sized>(
        &self,
        value: &T,
        buf: &mut [u8],
    ) -> Result<usize, SliceCodecError>;

    /// Decodes a value from `bytes`, borrowing from them where the value's type borrows.
    ///
    /// # Errors
    ///
    /// Returns [`SliceCodecError::Decode`] when the bytes are not a valid encoding of `T`.
    fn decode<'de, T: Deserialize<'de>>(&self, bytes: &'de [u8]) -> Result<T, SliceCodecError>;
}

/// What a [`SliceCodec`] reports when it fails. It holds no heap.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json-core")]
/// # mod demo {
/// use ruststream_core::codec::{JsonCore, SliceCodec, SliceCodecError};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Status<'a> {
///     state: &'a str,
/// }
///
/// /// A status report goes out whole or not at all: a frame that is too small is logged as such.
/// fn report(state: &str, frame: &mut [u8; 32]) -> Option<usize> {
///     match JsonCore.encode(&Status { state }, frame) {
///         Ok(written) => Some(written),
///         Err(SliceCodecError::BufferFull) => {
///             tracing::warn!(state, "the status does not fit into a frame");
///             None
///         }
///         Err(error) => {
///             tracing::warn!(%error, "status not sent");
///             None
///         }
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SliceCodecError {
    /// The serializer rejected the value.
    #[error("encode failed")]
    Encode,
    /// The value does not fit into the buffer.
    #[error("encode failed: the buffer is too small for the value")]
    BufferFull,
    /// The bytes are not a valid encoding of the requested type.
    #[error("decode failed")]
    Decode,
}
