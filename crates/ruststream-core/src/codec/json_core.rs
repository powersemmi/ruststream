//! [`JsonCore`]: JSON through the slice codec, without a heap.

use serde::{Deserialize, Serialize};
use serde_json_core::ser::Error as EncodeError;

use super::{SliceCodec, SliceCodecError};

/// JSON written and read by [`serde-json-core`](https://docs.rs/serde-json-core), without a heap.
///
/// It writes straight into the caller's buffer and decodes in place, so a `&str` field borrows
/// from the frame. A borrowed string that holds an escape sequence cannot be decoded in place
/// and is a decode error; an owned string type such as `heapless::String` takes it. A `Serialize`
/// implementation that fails with its own message panics inside `serde-json-core`, which has
/// nowhere to put the message.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json-core")]
/// # mod demo {
/// use ruststream_core::codec::{JsonCore, SliceCodec, SliceCodecError};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Setpoint<'a> {
///     zone: &'a str,
///     celsius: f32,
/// }
///
/// #[derive(Serialize)]
/// struct Applied<'a> {
///     zone: &'a str,
///     applied: bool,
/// }
///
/// /// A device handler: reads the setpoint from a JSON frame and answers with JSON written into
/// /// the reply frame.
/// fn on_setpoint(frame: &[u8], reply: &mut [u8; 64]) -> Result<usize, SliceCodecError> {
///     let setpoint: Setpoint<'_> = JsonCore.decode(frame)?;
///     tracing::info!(setpoint.zone, setpoint.celsius, "setpoint");
///     JsonCore.encode(&Applied { zone: setpoint.zone, applied: true }, reply)
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JsonCore;

impl SliceCodec for JsonCore {
    const CONTENT_TYPE: &'static str = "application/json";

    fn encode<T: Serialize + ?Sized>(
        &self,
        value: &T,
        buf: &mut [u8],
    ) -> Result<usize, SliceCodecError> {
        serde_json_core::to_slice(value, buf).map_err(|error| match error {
            EncodeError::BufferFull => SliceCodecError::BufferFull,
            _ => SliceCodecError::Encode,
        })
    }

    fn decode<'de, T: Deserialize<'de>>(&self, bytes: &'de [u8]) -> Result<T, SliceCodecError> {
        serde_json_core::from_slice(bytes)
            .map(|(value, _read)| value)
            .map_err(|_| SliceCodecError::Decode)
    }
}

#[cfg(test)]
mod tests {
    use core::ptr;
    use std::string::ToString;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Reading<'a> {
        sensor: &'a str,
        celsius: i16,
    }

    #[test]
    fn a_value_round_trips_borrowing_from_the_frame() {
        let mut frame = [0; 64];
        let written = JsonCore
            .encode(
                &Reading {
                    sensor: "boiler",
                    celsius: -4,
                },
                &mut frame,
            )
            .unwrap();
        assert_eq!(&frame[..written], br#"{"sensor":"boiler","celsius":-4}"#);
        let back: Reading<'_> = JsonCore.decode(&frame[..written]).unwrap();
        assert_eq!(
            back,
            Reading {
                sensor: "boiler",
                celsius: -4
            }
        );
        assert!(ptr::eq(back.sensor.as_ptr(), frame[11..].as_ptr()));
        assert_eq!(JsonCore::CONTENT_TYPE, "application/json");
    }

    #[test]
    fn a_buffer_too_small_is_reported_as_full() {
        let mut frame = [0; 8];
        let err = JsonCore
            .encode(
                &Reading {
                    sensor: "boiler",
                    celsius: 1,
                },
                &mut frame,
            )
            .unwrap_err();
        assert_eq!(err, SliceCodecError::BufferFull);
        assert_eq!(
            err.to_string(),
            "encode failed: the buffer is too small for the value"
        );
    }

    #[test]
    fn malformed_input_is_a_decode_error() {
        assert_eq!(
            JsonCore.decode::<Reading<'_>>(br#"{"sensor":"boiler""#),
            Err(SliceCodecError::Decode)
        );
        // Text after the value is not JSON either.
        assert_eq!(
            JsonCore.decode::<Reading<'_>>(br#"{"sensor":"a","celsius":1} x"#),
            Err(SliceCodecError::Decode)
        );
    }
}
