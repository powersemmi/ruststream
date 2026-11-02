//! The parts of the typed headers that differ between the build with `alloc` and the build
//! without: the text an error carries, the scalar a field becomes, the key of a map entry, and
//! the insert into the active header map, which is fallible in the build without `alloc`.
//!
//! Both builds expose the same names, so the serializer and the deserializer are written once.

use core::fmt::Display;

use super::SerializeHeadersError;
use crate::headers::Headers;

#[cfg(feature = "alloc")]
// The build without `alloc` fails where this one cannot; the callers are written once against the
// fallible signatures.
#[allow(clippy::unnecessary_wraps)]
mod form {
    use alloc::borrow::ToOwned;
    use alloc::string::{String, ToString};

    use bytes::Bytes;
    use bytes_utils::Str;

    use super::{Display, Headers, SerializeHeadersError};

    /// The text an error carries: a header or field name, a value as received.
    pub(in crate::typed_headers) type Text = String;
    /// The string-encoded value of one field.
    pub(in crate::typed_headers) type Scalar = Bytes;
    /// The name of a map entry.
    pub(in crate::typed_headers) type Key = String;

    pub(in crate::typed_headers) fn text(name: &str) -> Text {
        name.to_owned()
    }

    pub(in crate::typed_headers) fn text_lossy(bytes: &[u8]) -> Text {
        String::from_utf8_lossy(bytes).into_owned()
    }

    pub(in crate::typed_headers) fn text_of(value: &dyn Display) -> Text {
        value.to_string()
    }

    pub(in crate::typed_headers) fn scalar_display(
        value: &dyn Display,
    ) -> Result<Scalar, SerializeHeadersError> {
        Ok(Bytes::from(value.to_string()))
    }

    pub(in crate::typed_headers) fn scalar_bytes(
        bytes: &[u8],
    ) -> Result<Scalar, SerializeHeadersError> {
        Ok(Bytes::copy_from_slice(bytes))
    }

    pub(in crate::typed_headers) fn scalar_static(
        text: &'static str,
    ) -> Result<Scalar, SerializeHeadersError> {
        Ok(Bytes::from_static(text.as_bytes()))
    }

    pub(in crate::typed_headers) fn key(name: &str) -> Result<Key, SerializeHeadersError> {
        Ok(name.to_owned())
    }

    pub(in crate::typed_headers) fn key_of_char(name: char) -> Result<Key, SerializeHeadersError> {
        Ok(name.to_string())
    }

    /// A field name is a literal in the contract's own source, so the key costs nothing.
    pub(in crate::typed_headers) fn put_static(
        headers: &mut Headers,
        key: &'static str,
        value: Scalar,
    ) -> Result<(), SerializeHeadersError> {
        headers.insert(Str::from_static(key), value);
        Ok(())
    }

    pub(in crate::typed_headers) fn put_owned(
        headers: &mut Headers,
        key: Key,
        value: Scalar,
    ) -> Result<(), SerializeHeadersError> {
        headers.insert(key, value);
        Ok(())
    }
}

#[cfg(not(feature = "alloc"))]
mod form {
    use core::fmt::Write as _;

    use super::super::inline::Inline;
    use super::{Display, Headers, SerializeHeadersError};

    /// The room of an error text; a longer name or value is cut.
    const TEXT_ROOM: usize = 32;
    /// The room of one field value; a longer one is refused as `Full`.
    const SCALAR_ROOM: usize = 64;

    pub(in crate::typed_headers) type Text = Inline<TEXT_ROOM>;
    pub(in crate::typed_headers) type Scalar = Inline<SCALAR_ROOM>;
    pub(in crate::typed_headers) type Key = Inline<TEXT_ROOM>;

    pub(in crate::typed_headers) fn text(name: &str) -> Text {
        Inline::truncated(name)
    }

    pub(in crate::typed_headers) fn text_lossy(bytes: &[u8]) -> Text {
        Inline::lossy(bytes)
    }

    pub(in crate::typed_headers) fn text_of(value: &dyn Display) -> Text {
        Inline::display(value)
    }

    pub(in crate::typed_headers) fn scalar_display(
        value: &dyn Display,
    ) -> Result<Scalar, SerializeHeadersError> {
        let mut run = Scalar::new();
        write!(run, "{value}").map_err(|_| SerializeHeadersError::Full)?;
        Ok(run)
    }

    pub(in crate::typed_headers) fn scalar_bytes(
        bytes: &[u8],
    ) -> Result<Scalar, SerializeHeadersError> {
        let mut run = Scalar::new();
        run.try_push(bytes)
            .map_err(|_| SerializeHeadersError::Full)?;
        Ok(run)
    }

    pub(in crate::typed_headers) fn scalar_static(
        text: &'static str,
    ) -> Result<Scalar, SerializeHeadersError> {
        scalar_bytes(text.as_bytes())
    }

    pub(in crate::typed_headers) fn key(name: &str) -> Result<Key, SerializeHeadersError> {
        let mut run = Key::new();
        run.try_push(name.as_bytes())
            .map_err(|_| SerializeHeadersError::Full)?;
        Ok(run)
    }

    pub(in crate::typed_headers) fn key_display(
        name: &dyn Display,
    ) -> Result<Key, SerializeHeadersError> {
        let mut run = Key::new();
        write!(run, "{name}").map_err(|_| SerializeHeadersError::Full)?;
        Ok(run)
    }

    pub(in crate::typed_headers) fn key_of_char(name: char) -> Result<Key, SerializeHeadersError> {
        let mut buf = [0; 4];
        key(name.encode_utf8(&mut buf))
    }

    pub(in crate::typed_headers) fn put_static(
        headers: &mut Headers,
        key: &'static str,
        value: Scalar,
    ) -> Result<(), SerializeHeadersError> {
        headers
            .insert(key, value.as_bytes())
            .map_err(|_| SerializeHeadersError::Full)
    }

    pub(in crate::typed_headers) fn put_owned(
        headers: &mut Headers,
        key: Key,
        value: Scalar,
    ) -> Result<(), SerializeHeadersError> {
        headers
            .insert(&key, value.as_bytes())
            .map_err(|_| SerializeHeadersError::Full)
    }
}

#[cfg(not(feature = "alloc"))]
pub(super) use form::key_display;
pub(super) use form::{
    Key, Scalar, Text, key, key_of_char, put_owned, put_static, scalar_bytes, scalar_display,
    scalar_static, text, text_lossy, text_of,
};
