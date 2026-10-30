//! The headers of a message as a trait: [`HeaderStore`] reads them, [`HeaderStoreMut`] writes
//! them, whatever storage holds them.

use core::error::Error as StdError;
use core::str;

use serde::{Deserialize, Serialize};

use crate::typed_headers::{
    HeadersDeserializer, ReadHeadersError, StoreSerializer, WriteHeadersError,
};

/// Read access to the headers of a message: names compared without regard to ASCII case, values
/// as bytes.
///
/// The core's [`HeaderMap`](crate::HeaderMap) implements it over the heap, and
/// [`FixedHeaderMap`](crate::FixedHeaderMap) (feature `heapless`) in storage of a fixed size. Code
/// that only reads headers takes `impl HeaderStore`, so it serves both. Every name the trait hands
/// out is lowercase.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "heapless")]
/// # mod demo {
/// use ruststream_core::{FixedHeaderMap, HeaderStore};
///
/// /// The handler side: picks the decoder by the content type, whatever headers the transport
/// /// keeps.
/// fn is_json(headers: &impl HeaderStore) -> bool {
///     headers
///         .get_str("Content-Type")
///         .is_some_and(|kind| kind.starts_with("application/json"))
/// }
///
/// fn on_frame(headers: &FixedHeaderMap<4, 64>, payload: &[u8]) {
///     if is_json(headers) {
///         tracing::info!(bytes = payload.len(), "a JSON reading");
///     }
/// }
/// # }
/// # fn main() {}
/// ```
pub trait HeaderStore {
    /// Returns the raw bytes of a header value, or `None` if the header is absent.
    fn get(&self, name: &str) -> Option<&[u8]>;

    /// Returns the number of headers.
    fn len(&self) -> usize;

    /// Iterates over `(name, value)` pairs, names in their lowercase form.
    fn iter(&self) -> impl Iterator<Item = (&str, &[u8])>;

    /// Returns the value of a header decoded as UTF-8, or `None` if absent or not valid UTF-8.
    fn get_str(&self, name: &str) -> Option<&str> {
        str::from_utf8(self.get(name)?).ok()
    }

    /// Returns `true` if the given header is present.
    fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Returns `true` if no headers are present.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Parses the headers into a typed contract `T`, borrowing text and bytes from them.
    ///
    /// `T` is a flat struct (or string-keyed map): each field reads the header of the same name
    /// and parses its string-encoded value into what the field expects. Integers and floats parse
    /// from their string form, booleans from `true` / `false`, unit-only enums from the variant
    /// name, and byte fields take the raw value. A `&str` or `&[u8]` field borrows from the
    /// store. An `Option` field is `None` when the header is absent. Nothing is allocated, and
    /// the error borrows the offending header's name and value.
    ///
    /// # Errors
    ///
    /// Returns [`ReadHeadersError`] when a required header is missing, a value does not parse
    /// into its field's type, or `T` asks for a shape header values cannot carry (nested
    /// structs, sequences, data-carrying enum variants).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "heapless")]
    /// # mod demo {
    /// use ruststream_core::{FixedHeaderMap, HeaderStore, ReadHeadersError};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct ChunkMeta<'a> {
    ///     task: &'a str,
    ///     chunk_no: u32,
    ///     last: Option<bool>,
    /// }
    ///
    /// /// A device handler that stores a chunk under its task, reading the contract from the
    /// /// headers its transport decoded without a heap.
    /// fn store_chunk<'a>(
    ///     headers: &'a FixedHeaderMap<8, 128>,
    ///     chunk: &[u8],
    /// ) -> Result<(), ReadHeadersError<'a>> {
    ///     let meta: ChunkMeta<'a> = headers.read_typed()?;
    ///     let last = meta.last.unwrap_or(false);
    ///     tracing::info!(meta.task, meta.chunk_no, last, bytes = chunk.len(), "stored");
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn read_typed<'de, T: Deserialize<'de>>(&'de self) -> Result<T, ReadHeadersError<'de>>
    where
        Self: Sized,
    {
        T::deserialize(HeadersDeserializer::new(self))
    }
}

/// Write access to the headers of a message.
///
/// An insert names its failure through [`InsertError`](Self::InsertError): a store of a fixed
/// size reports that it is full, and a store on the heap never fails. A name is stored in its
/// ASCII lowercase form, and an insert under a name already present replaces its value.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "heapless")]
/// # mod demo {
/// use ruststream_core::{FixedHeaderMap, HeaderStoreMut};
///
/// /// A transport's publish step: stamps the frame with the device it left, whatever headers
/// /// the frame keeps. A full store is the caller's error to handle, not a panic.
/// fn stamp<Headers>(headers: &mut Headers, device: &str) -> Result<(), Headers::InsertError>
/// where
///     Headers: HeaderStoreMut,
/// {
///     headers.insert("Device", device.as_bytes())
/// }
///
/// fn frame_headers(device: &str) -> Option<FixedHeaderMap<4, 64>> {
///     let mut headers = FixedHeaderMap::new();
///     stamp(&mut headers, device).ok()?;
///     Some(headers)
/// }
/// # }
/// # fn main() {}
/// ```
pub trait HeaderStoreMut: HeaderStore {
    /// What a failed insert reports.
    type InsertError: StdError + 'static;

    /// Inserts a header value, replacing the value already stored under that name.
    ///
    /// # Errors
    ///
    /// Returns [`InsertError`](Self::InsertError) when the store has no room for the entry; the
    /// store is then unchanged.
    fn insert(&mut self, name: &str, value: &[u8]) -> Result<(), Self::InsertError>;

    /// Removes a header by name; returns `true` if it was present.
    fn remove(&mut self, name: &str) -> bool;

    /// Serializes a typed contract into the headers, one entry per field.
    ///
    /// The mirror of [`HeaderStore::read_typed`]: every field of a flat struct becomes a header
    /// named after the field, with its value string-encoded (numbers and booleans in their
    /// display form, unit-only enums as the variant name, byte fields as the raw value). An
    /// `Option` field that is `None` inserts nothing. Headers under other names are kept. Numbers
    /// are formatted on the stack, so nothing is allocated.
    ///
    /// # Errors
    ///
    /// Returns [`WriteHeadersError`] when the value is not a struct, a field holds a non-scalar,
    /// or the store has no room for a field. The fields before the failing one stay written.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "heapless")]
    /// # mod demo {
    /// use ruststream_core::{FixedHeaderMap, HeaderStoreMut, InsertHeaderError, WriteHeadersError};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Origin<'a> {
    ///     device: &'a str,
    ///     battery: u8,
    /// }
    ///
    /// /// A device's publish step: the frame's headers carry where it came from.
    /// fn frame_headers(
    ///     device: &str,
    ///     battery: u8,
    /// ) -> Result<FixedHeaderMap<4, 64>, WriteHeadersError<InsertHeaderError>> {
    ///     let mut headers = FixedHeaderMap::new();
    ///     headers.write_typed(&Origin { device, battery })?;
    ///     Ok(headers)
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn write_typed<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), WriteHeadersError<Self::InsertError>>
    where
        Self: Sized,
    {
        value.serialize(StoreSerializer::new(self))
    }
}
