//! The headers of a message as a trait: [`HeaderStore`] reads them, [`HeaderStoreMut`] writes
//! them, whatever storage holds them.

use core::error::Error as StdError;
use core::str;

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
    type InsertError: StdError;

    /// Inserts a header value, replacing the value already stored under that name.
    ///
    /// # Errors
    ///
    /// Returns [`InsertError`](Self::InsertError) when the store has no room for the entry; the
    /// store is then unchanged.
    fn insert(&mut self, name: &str, value: &[u8]) -> Result<(), Self::InsertError>;

    /// Removes a header by name; returns `true` if it was present.
    fn remove(&mut self, name: &str) -> bool;
}
