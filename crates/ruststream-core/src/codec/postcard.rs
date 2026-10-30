//! [`Postcard`]: the postcard format through the slice codec.

use postcard::Error;
use serde::{Deserialize, Serialize};

use super::{SliceCodec, SliceCodecError};

/// The [postcard](https://docs.rs/postcard) format, compact and without a schema on the wire.
///
/// It writes straight into the caller's buffer and decodes in place, so a `&str` or `&[u8]`
/// field borrows from the frame. Both ends must agree on the type: the encoding carries no field
/// names.
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
/// struct Command<'a> {
///     valve: &'a str,
///     open: bool,
/// }
///
/// /// A device handler: decodes the command in place and answers with an acknowledgement
/// /// written into the reply frame.
/// fn on_command(frame: &[u8], reply: &mut [u8; 16]) -> Result<usize, SliceCodecError> {
///     let command: Command<'_> = Postcard.decode(frame)?;
///     tracing::info!(command.valve, command.open, "valve set");
///     Postcard.encode(&true, reply)
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Postcard;

impl SliceCodec for Postcard {
    const CONTENT_TYPE: &'static str = "application/postcard";

    fn encode<T: Serialize + ?Sized>(
        &self,
        value: &T,
        buf: &mut [u8],
    ) -> Result<usize, SliceCodecError> {
        match postcard::to_slice(value, buf) {
            Ok(written) => Ok(written.len()),
            Err(Error::SerializeBufferFull) => Err(SliceCodecError::BufferFull),
            Err(_) => Err(SliceCodecError::Encode),
        }
    }

    fn decode<'de, T: Deserialize<'de>>(&self, bytes: &'de [u8]) -> Result<T, SliceCodecError> {
        postcard::from_bytes(bytes).map_err(|_| SliceCodecError::Decode)
    }
}

#[cfg(test)]
mod tests {
    use core::ptr;
    use std::string::ToString;

    use serde::ser::Error as SerError;
    use serde::{Deserialize, Serialize, Serializer};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Reading<'a> {
        sensor: &'a str,
        celsius: i16,
    }

    #[test]
    fn a_value_round_trips_borrowing_from_the_frame() {
        let mut frame = [0; 32];
        let written = Postcard
            .encode(
                &Reading {
                    sensor: "boiler",
                    celsius: -4,
                },
                &mut frame,
            )
            .unwrap();
        // A length byte and six of text, then the zigzag varint of -4.
        assert_eq!(&frame[..written], b"\x06boiler\x07");
        let back: Reading<'_> = Postcard.decode(&frame[..written]).unwrap();
        assert_eq!(
            back,
            Reading {
                sensor: "boiler",
                celsius: -4
            }
        );
        assert!(ptr::eq(back.sensor.as_ptr(), frame[1..].as_ptr()));
    }

    #[test]
    fn a_buffer_too_small_is_reported_as_full() {
        let mut frame = [0; 4];
        let err = Postcard
            .encode(
                &Reading {
                    sensor: "boiler",
                    celsius: 1,
                },
                &mut frame,
            )
            .unwrap_err();
        assert_eq!(err, SliceCodecError::BufferFull);
    }

    #[test]
    fn a_value_the_format_cannot_carry_is_an_encode_error() {
        struct Refuses;
        impl Serialize for Refuses {
            fn serialize<S: Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
                Err(SerError::custom("no"))
            }
        }
        assert_eq!(
            Postcard.encode(&Refuses, &mut [0; 4]),
            Err(SliceCodecError::Encode)
        );
    }

    #[test]
    fn malformed_input_is_a_decode_error() {
        // The length says six bytes of text, and two follow.
        let err = Postcard.decode::<Reading<'_>>(b"\x06bo").unwrap_err();
        assert_eq!(err, SliceCodecError::Decode);
        assert_eq!(err.to_string(), "decode failed");
    }
}
