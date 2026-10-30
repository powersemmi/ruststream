//! [`FixedHeaderMap`]: the headers of a message in storage of a fixed size.

use core::{fmt, str};

use heapless::Vec;
use thiserror::Error;

use crate::header_store::{HeaderStore, HeaderStoreMut};

/// Headers in storage of a fixed size, for a device without a heap.
///
/// `ENTRIES` bounds the number of headers and `BYTES` the bytes of their names and values
/// together, so a store takes `BYTES` plus two words per entry wherever it lives, and never
/// allocates. A header store sized to its transport's frame limit holds every frame the
/// transport accepts. An insert that does not fit is an [`InsertHeaderError`], and the store is
/// then unchanged.
///
/// It reads and writes like the core's [`HeaderMap`](crate::HeaderMap): names are stored in their
/// ASCII lowercase form and compared without regard to case, an insert under a present name
/// replaces the value, and two stores are equal when they hold the same entries in whatever
/// order. A lookup scans the entries, which over a handful of headers is cheaper than hashing.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "heapless")]
/// # mod demo {
/// use ruststream_core::{FixedHeaderMap, HeaderStore, InsertHeaderError};
///
/// /// A delivery of a device transport: the headers its frame carried, decoded into storage
/// /// of a fixed size, and the payload borrowed from the frame.
/// pub struct Delivery<'frame> {
///     headers: FixedHeaderMap<8, 256>,
///     payload: &'frame [u8],
/// }
///
/// impl<'frame> Delivery<'frame> {
///     /// Decodes a frame of `name=value` header lines ended by an empty line, then the payload.
///     pub fn parse(frame: &'frame [u8]) -> Result<Self, InsertHeaderError> {
///         let mut headers = FixedHeaderMap::new();
///         let mut rest = frame;
///         while let Some(end) = rest.iter().position(|&b| b == b'\n') {
///             let (line, tail) = (&rest[..end], &rest[end + 1..]);
///             rest = tail;
///             let Some(eq) = line.iter().position(|&b| b == b'=') else {
///                 break;
///             };
///             if let Ok(name) = core::str::from_utf8(&line[..eq]) {
///                 headers.insert(name, &line[eq + 1..])?;
///             }
///         }
///         Ok(Self { headers, payload: rest })
///     }
///
///     /// The headers, as the broker's delivery hands them to the runtime.
///     pub fn headers(&self) -> &FixedHeaderMap<8, 256> {
///         &self.headers
///     }
///
///     /// The payload bytes.
///     pub fn payload(&self) -> &[u8] {
///         self.payload
///     }
/// }
///
/// fn on_frame(frame: &[u8]) {
///     match Delivery::parse(frame) {
///         Ok(delivery) => {
///             let kind = delivery.headers().get_str("content-type");
///             tracing::info!(?kind, bytes = delivery.payload().len(), "delivered");
///         }
///         Err(error) => tracing::warn!(%error, "frame dropped"),
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Default)]
pub struct FixedHeaderMap<const ENTRIES: usize, const BYTES: usize> {
    // Names and values back to back, in entry order: entry `i` starts where entry `i - 1` ends.
    bytes: Vec<u8, BYTES>,
    entries: Vec<Span, ENTRIES>,
}

/// Where one entry sits in the byte storage: its name's length, then its value's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Span {
    name: usize,
    value: usize,
}

/// The error of an insert into a [`FixedHeaderMap`] that has no room for the entry.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "heapless")]
/// # mod demo {
/// use ruststream_core::{FixedHeaderMap, InsertHeaderError};
///
/// /// Stamps a trace id onto a frame's headers; one that does not fit is logged and left off,
/// /// so the frame still goes out.
/// fn stamp_trace(headers: &mut FixedHeaderMap<4, 64>, trace: &str) {
///     if let Err(error) = headers.insert("trace-id", trace.as_bytes()) {
///         match error {
///             InsertHeaderError::Entries => tracing::warn!(%error, "every header slot taken"),
///             InsertHeaderError::Bytes => tracing::warn!(%error, trace, "the trace id is too long"),
///             _ => tracing::warn!(%error, "trace id left off"),
///         }
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum InsertHeaderError {
    /// Every entry of the store is taken.
    #[error("the header store has no room for another entry")]
    Entries,
    /// The entry's name and value do not fit into the bytes the store has left.
    #[error("the header store has no room for the entry's bytes")]
    Bytes,
}

impl<const ENTRIES: usize, const BYTES: usize> FixedHeaderMap<ENTRIES, BYTES> {
    /// Returns an empty header store.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "heapless")]
    /// # mod demo {
    /// use ruststream_core::FixedHeaderMap;
    ///
    /// /// The headers every frame of a device starts from, built at compile time.
    /// static EMPTY: FixedHeaderMap<4, 64> = FixedHeaderMap::new();
    ///
    /// fn reply_headers(correlation_id: &[u8]) -> FixedHeaderMap<4, 64> {
    ///     let mut headers = EMPTY.clone();
    ///     if headers.insert("correlation-id", correlation_id).is_err() {
    ///         tracing::warn!("correlation id left off the reply");
    ///     }
    ///     headers
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            entries: Vec::new(),
        }
    }

    /// Inserts a header value, replacing the value already stored under that name.
    ///
    /// The name is stored in its ASCII lowercase form.
    ///
    /// # Errors
    ///
    /// Returns [`InsertHeaderError::Entries`] when a new name finds every entry taken, and
    /// [`InsertHeaderError::Bytes`] when the name and value do not fit into the bytes left. The
    /// store is then unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "heapless")]
    /// # mod demo {
    /// use ruststream_core::{FixedHeaderMap, InsertHeaderError};
    ///
    /// /// The headers of a reply: the request's correlation id, and the content type.
    /// fn reply_headers(correlation_id: &[u8]) -> Result<FixedHeaderMap<4, 64>, InsertHeaderError> {
    ///     let mut headers = FixedHeaderMap::new();
    ///     headers.insert("Content-Type", b"application/json")?;
    ///     headers.insert("correlation-id", correlation_id)?;
    ///     Ok(headers)
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn insert(&mut self, name: &str, value: &[u8]) -> Result<(), InsertHeaderError> {
        if let Some((index, start, span)) = self.position(name) {
            return self.replace(index, start + span.name, span.value, value);
        }
        self.entries
            .push(Span {
                name: name.len(),
                value: value.len(),
            })
            .map_err(|_| InsertHeaderError::Entries)?;
        let start = self.bytes.len();
        // Each step that can run out of room leaves the store as it found it.
        if self.bytes.extend_from_slice(name.as_bytes()).is_err() {
            self.entries.pop();
            return Err(InsertHeaderError::Bytes);
        }
        if self.bytes.extend_from_slice(value).is_err() {
            self.bytes.truncate(start);
            self.entries.pop();
            return Err(InsertHeaderError::Bytes);
        }
        self.bytes[start..start + name.len()].make_ascii_lowercase();
        Ok(())
    }

    /// Writes `value` over the `old` bytes of entry `index`'s value at `at`, moving the entries
    /// after it by the difference.
    fn replace(
        &mut self,
        index: usize,
        at: usize,
        old: usize,
        value: &[u8],
    ) -> Result<(), InsertHeaderError> {
        let len = self.bytes.len();
        let tail = at + old..len;
        if value.len() > old {
            let grown = len + (value.len() - old);
            self.bytes
                .resize(grown, 0)
                .map_err(|_| InsertHeaderError::Bytes)?;
            self.bytes.copy_within(tail, at + value.len());
        } else {
            self.bytes.copy_within(tail, at + value.len());
            self.bytes.truncate(len - (old - value.len()));
        }
        self.bytes[at..at + value.len()].copy_from_slice(value);
        self.entries[index].value = value.len();
        Ok(())
    }

    /// The entry named `name`, compared without regard to case: its index, where its bytes
    /// start, and its span. Stored names are lowercase, so this is the lookup a lowercased copy
    /// of `name` would make, without the copy.
    fn position(&self, name: &str) -> Option<(usize, usize, Span)> {
        let mut start = 0;
        for (index, span) in self.entries.iter().enumerate() {
            if self.bytes[start..start + span.name].eq_ignore_ascii_case(name.as_bytes()) {
                return Some((index, start, *span));
            }
            start += span.name + span.value;
        }
        None
    }

    /// Returns the raw bytes of a header value, or `None` if the header is absent.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let (_, start, span) = self.position(name)?;
        let at = start + span.name;
        Some(&self.bytes[at..at + span.value])
    }

    /// Returns the value of a header decoded as UTF-8, or `None` if absent or not valid UTF-8.
    #[must_use]
    pub fn get_str(&self, name: &str) -> Option<&str> {
        str::from_utf8(self.get(name)?).ok()
    }

    /// Removes a header by name; returns `true` if it was present.
    pub fn remove(&mut self, name: &str) -> bool {
        let Some((index, start, span)) = self.position(name) else {
            return false;
        };
        let len = self.bytes.len();
        let end = start + span.name + span.value;
        self.bytes.copy_within(end..len, start);
        self.bytes.truncate(len - (end - start));
        self.entries.remove(index);
        true
    }

    /// Returns `true` if the given header is present.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Returns the number of headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no headers are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over `(name, value)` pairs, names in their lowercase form.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        let mut start = 0;
        self.entries.iter().map(move |span| {
            let value = start + span.name;
            let end = value + span.value;
            // A name is copied in from a `&str` and only ASCII-lowercased, so it is always UTF-8
            // and the check never falls back.
            let name = str::from_utf8(&self.bytes[start..value]).unwrap_or_default();
            start = end;
            (name, &self.bytes[value..end])
        })
    }
}

impl<const ENTRIES: usize, const BYTES: usize> fmt::Debug for FixedHeaderMap<ENTRIES, BYTES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// Equal when both hold the same entries, in whatever order they were inserted.
impl<const ENTRIES: usize, const BYTES: usize> PartialEq for FixedHeaderMap<ENTRIES, BYTES> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(name, value)| other.get(name) == Some(value))
    }
}

impl<const ENTRIES: usize, const BYTES: usize> Eq for FixedHeaderMap<ENTRIES, BYTES> {}

impl<const ENTRIES: usize, const BYTES: usize> HeaderStore for FixedHeaderMap<ENTRIES, BYTES> {
    #[inline]
    fn get(&self, name: &str) -> Option<&[u8]> {
        Self::get(self, name)
    }

    #[inline]
    fn len(&self) -> usize {
        Self::len(self)
    }

    #[inline]
    fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        Self::iter(self)
    }
}

impl<const ENTRIES: usize, const BYTES: usize> HeaderStoreMut for FixedHeaderMap<ENTRIES, BYTES> {
    type InsertError = InsertHeaderError;

    #[inline]
    fn insert(&mut self, name: &str, value: &[u8]) -> Result<(), InsertHeaderError> {
        Self::insert(self, name, value)
    }

    #[inline]
    fn remove(&mut self, name: &str) -> bool {
        Self::remove(self, name)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use std::vec::Vec as StdVec;

    use super::*;

    #[test]
    fn insert_and_get_compare_names_without_case() {
        let mut headers = FixedHeaderMap::<4, 64>::new();
        headers.insert("Content-Type", b"application/json").unwrap();
        assert_eq!(
            headers.get("content-type"),
            Some(b"application/json".as_slice())
        );
        assert_eq!(headers.get_str("CONTENT-TYPE"), Some("application/json"));
        assert!(headers.contains("Content-type"));
        assert_eq!(headers.get("accept"), None);
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn names_are_stored_lowercase_in_insertion_order() {
        let mut headers = FixedHeaderMap::<4, 64>::new();
        headers.insert("X-B", b"2").unwrap();
        headers.insert("x-a", b"1").unwrap();
        let pairs: StdVec<_> = headers.iter().collect();
        assert_eq!(
            pairs,
            vec![("x-b", b"2".as_slice()), ("x-a", b"1".as_slice())]
        );
    }

    #[test]
    fn insert_under_a_present_name_replaces_the_value_in_place() {
        let mut headers = FixedHeaderMap::<4, 32>::new();
        headers.insert("a", b"1").unwrap();
        headers.insert("b", b"22").unwrap();
        headers.insert("c", b"3").unwrap();

        headers.insert("B", b"longer value").unwrap();
        assert_eq!(headers.get("b"), Some(b"longer value".as_slice()));
        headers.insert("b", b"").unwrap();
        assert_eq!(headers.get("b"), Some(b"".as_slice()));

        let pairs: StdVec<_> = headers.iter().collect();
        assert_eq!(
            pairs,
            vec![
                ("a", b"1".as_slice()),
                ("b", b"".as_slice()),
                ("c", b"3".as_slice())
            ]
        );
    }

    #[test]
    fn remove_drops_the_entry_and_keeps_the_rest() {
        let mut headers = FixedHeaderMap::<4, 32>::new();
        headers.insert("a", b"1").unwrap();
        headers.insert("b", b"2").unwrap();
        headers.insert("c", b"3").unwrap();
        assert!(headers.remove("B"));
        assert!(!headers.remove("b"));
        let pairs: StdVec<_> = headers.iter().collect();
        assert_eq!(pairs, vec![("a", b"1".as_slice()), ("c", b"3".as_slice())]);
        // The freed bytes are free again.
        headers.insert("d", b"4").unwrap();
        assert_eq!(headers.get("d"), Some(b"4".as_slice()));
    }

    #[test]
    fn a_full_store_reports_which_capacity_ran_out_and_stays_unchanged() {
        let mut entries = FixedHeaderMap::<1, 64>::new();
        entries.insert("a", b"1").unwrap();
        assert_eq!(entries.insert("b", b"2"), Err(InsertHeaderError::Entries));
        // A replacement takes no new entry.
        entries.insert("a", b"9").unwrap();
        assert_eq!(entries.get("a"), Some(b"9".as_slice()));

        let mut bytes = FixedHeaderMap::<4, 8>::new();
        bytes.insert("ab", b"1234").unwrap();
        assert_eq!(bytes.insert("cd", b"34"), Err(InsertHeaderError::Bytes));
        assert_eq!(
            bytes.insert("a-long-name", b""),
            Err(InsertHeaderError::Bytes)
        );
        assert_eq!(
            bytes.insert("ab", b"1234567"),
            Err(InsertHeaderError::Bytes)
        );
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes.get("ab"), Some(b"1234".as_slice()));
        // Exactly full fits.
        bytes.insert("ab", b"123456").unwrap();
        assert_eq!(bytes.get("ab"), Some(b"123456".as_slice()));
    }

    #[test]
    fn equality_ignores_insertion_order() {
        let mut a = FixedHeaderMap::<4, 32>::new();
        a.insert("x-a", b"1").unwrap();
        a.insert("x-b", b"2").unwrap();
        let mut b = FixedHeaderMap::<4, 32>::new();
        b.insert("X-B", b"2").unwrap();
        b.insert("x-a", b"1").unwrap();
        assert_eq!(a, b);
        b.insert("x-a", b"3").unwrap();
        assert_ne!(a, b);
        b.remove("x-a");
        assert_ne!(a, b);
    }

    #[test]
    fn debug_prints_the_entries_as_a_map() {
        let mut headers = FixedHeaderMap::<2, 16>::new();
        headers.insert("k", b"v").unwrap();
        assert_eq!(std::format!("{headers:?}"), "{\"k\": [118]}");
    }

    #[test]
    fn the_traits_reach_the_same_entries() {
        fn through_traits<Store: HeaderStoreMut>(store: &mut Store) -> (bool, usize) {
            assert!(store.insert("Name", b"value").is_ok());
            let found = store.get_str("name") == Some("value") && store.contains("NAME");
            let removed = store.remove("name");
            (found && removed && store.is_empty(), store.iter().count())
        }
        assert_eq!(
            through_traits(&mut FixedHeaderMap::<2, 16>::new()),
            (true, 0)
        );
    }
}
