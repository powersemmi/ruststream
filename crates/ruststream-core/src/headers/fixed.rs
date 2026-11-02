//! Message headers in fixed storage, for a build without an allocator.

use core::error::Error;
use core::fmt;
use core::str;

/// The header map is full: it holds `ENTRIES` entries already, or the new entry does not fit in
/// the `BYTES` of its arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadersFull;

impl fmt::Display for HeadersFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the header map is full: no free entry or no room for the bytes")
    }
}

impl Error for HeadersFull {}

#[derive(Clone, Copy)]
struct Entry {
    start: u16,
    key_len: u16,
    value_len: u16,
}

/// Case-insensitive map of broker-message headers in fixed storage.
pub struct FixedHeaders<const ENTRIES: usize, const BYTES: usize> {
    entries: [Entry; ENTRIES],
    count: usize,
    used: usize,
    arena: [u8; BYTES],
}

impl<const ENTRIES: usize, const BYTES: usize> Clone for FixedHeaders<ENTRIES, BYTES> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries,
            count: self.count,
            used: self.used,
            arena: self.arena,
        }
    }
}

impl<const ENTRIES: usize, const BYTES: usize> Default for FixedHeaders<ENTRIES, BYTES> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ENTRIES: usize, const BYTES: usize> fmt::Debug for FixedHeaders<ENTRIES, BYTES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<const ENTRIES: usize, const BYTES: usize> PartialEq for FixedHeaders<ENTRIES, BYTES> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|(k, v)| other.get(k) == Some(v))
    }
}

impl<const ENTRIES: usize, const BYTES: usize> Eq for FixedHeaders<ENTRIES, BYTES> {}

impl<const ENTRIES: usize, const BYTES: usize> FixedHeaders<ENTRIES, BYTES> {
    /// Returns an empty header map.
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                BYTES <= u16::MAX as usize,
                "the arena is addressed by u16 offsets"
            );
        }
        Self {
            entries: [Entry {
                start: 0,
                key_len: 0,
                value_len: 0,
            }; ENTRIES],
            count: 0,
            used: 0,
            arena: [0; BYTES],
        }
    }

    fn live(&self) -> &[Entry] {
        &self.entries[..self.count]
    }

    fn key_of(&self, entry: Entry) -> &[u8] {
        let start = usize::from(entry.start);
        &self.arena[start..start + usize::from(entry.key_len)]
    }

    fn value_of(&self, entry: Entry) -> &[u8] {
        let start = usize::from(entry.start) + usize::from(entry.key_len);
        &self.arena[start..start + usize::from(entry.value_len)]
    }

    fn size_of_entry(entry: Entry) -> usize {
        usize::from(entry.key_len) + usize::from(entry.value_len)
    }

    fn position(&self, name: &str) -> Option<usize> {
        self.live()
            .iter()
            .position(|&entry| self.key_of(entry).eq_ignore_ascii_case(name.as_bytes()))
    }

    fn remove_at(&mut self, index: usize) {
        let gone = self.entries[index];
        let size = Self::size_of_entry(gone);
        let start = usize::from(gone.start);
        self.arena.copy_within(start + size..self.used, start);
        self.used -= size;
        self.entries.copy_within(index + 1..self.count, index);
        self.count -= 1;
        for entry in &mut self.entries[index..self.count] {
            // The tail moved down by `size`, which is below `u16::MAX` like every offset.
            entry.start -= gone.key_len + gone.value_len;
        }
    }

    /// Inserts a header value, replacing the value under that key if any.
    ///
    /// # Errors
    ///
    /// Returns [`HeadersFull`] when the map cannot take the entry; the map is left as it was.
    pub fn insert(&mut self, name: &str, value: impl AsRef<[u8]>) -> Result<(), HeadersFull> {
        let value = value.as_ref();
        let existing = self.position(name);
        let freed = existing.map_or(0, |index| Self::size_of_entry(self.entries[index]));
        let needed = name.len() + value.len();
        let has_slot = existing.is_some() || self.count < ENTRIES;
        let (Ok(key_len), Ok(value_len)) = (u16::try_from(name.len()), u16::try_from(value.len()))
        else {
            return Err(HeadersFull);
        };
        if !has_slot || self.used - freed + needed > BYTES {
            return Err(HeadersFull);
        }
        if let Some(index) = existing {
            self.remove_at(index);
        }
        let start = self.used;
        let key_end = start + name.len();
        self.arena[start..key_end].copy_from_slice(name.as_bytes());
        self.arena[start..key_end].make_ascii_lowercase();
        self.arena[key_end..key_end + value.len()].copy_from_slice(value);
        self.entries[self.count] = Entry {
            // The room check above keeps `start` below `BYTES`, which fits `u16`.
            start: u16::try_from(start).map_err(|_| HeadersFull)?,
            key_len,
            value_len,
        };
        self.count += 1;
        self.used += needed;
        Ok(())
    }

    /// Returns the raw bytes of a header value, or `None` if the header is absent.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.position(name)
            .map(|index| self.value_of(self.entries[index]))
    }

    /// Returns a header value decoded as UTF-8, or `None` if absent or not valid UTF-8.
    #[must_use]
    pub fn get_str(&self, name: &str) -> Option<&str> {
        str::from_utf8(self.get(name)?).ok()
    }

    /// Removes a header by name; returns `true` if it was present.
    pub fn remove(&mut self, name: &str) -> bool {
        self.position(name).is_some_and(|index| {
            self.remove_at(index);
            true
        })
    }

    /// Returns `true` if the given header is present.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.position(name).is_some()
    }

    /// Returns the number of headers.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Returns `true` if no headers are present.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterates over `(name, value)` pairs. Names are returned in their normalized lowercase form.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.live().iter().map(|&entry| {
            // Keys are copied from a `&str` and only ASCII-lowercased, so they stay UTF-8.
            (
                str::from_utf8(self.key_of(entry)).unwrap_or_default(),
                self.value_of(entry),
            )
        })
    }

    /// Returns the value of the `content-type` header decoded as UTF-8.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.get_str("content-type")
    }

    /// Returns the value of the `correlation-id` header decoded as UTF-8.
    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        self.get_str("correlation-id")
    }

    /// Returns the value of the `reply-to` header decoded as UTF-8.
    #[must_use]
    pub fn reply_to(&self) -> Option<&str> {
        self.get_str("reply-to")
    }

    /// Returns the value of the `message-id` header decoded as UTF-8.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.get_str("message-id")
    }

    /// Writes every entry of `other` over this map, keeping the entries `other` does not name.
    ///
    /// # Errors
    ///
    /// Returns [`HeadersFull`] when the merged map does not fit; this map is left as it was.
    #[doc(hidden)]
    pub fn overwrite_with(&mut self, other: &Self) -> Result<(), HeadersFull> {
        // Merged on a copy so a map that does not fit is left as it was.
        let mut merged = self.clone();
        for (key, value) in other.iter() {
            merged.insert(key, value)?;
        }
        *self = merged;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::format;
    use std::vec::Vec;

    use super::*;

    type Small = FixedHeaders<3, 32>;

    #[test]
    fn insert_and_get_are_case_insensitive() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("Content-Type", "application/json")?;
        assert_eq!(h.get("content-type"), Some(b"application/json".as_slice()));
        assert_eq!(h.get("CONTENT-TYPE"), Some(b"application/json".as_slice()));
        assert_eq!(h.get_str("Content-Type"), Some("application/json"));
        assert_eq!(h.get("absent"), None);
        Ok(())
    }

    #[test]
    fn inserting_a_present_key_replaces_its_value() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("x-a", "1")?;
        h.insert("x-b", "2")?;
        h.insert("X-A", "longer")?;
        assert_eq!(h.len(), 2);
        assert_eq!(h.get_str("x-a"), Some("longer"));
        assert_eq!(h.get_str("x-b"), Some("2"));
        Ok(())
    }

    #[test]
    fn a_full_entry_table_refuses_a_new_key_and_keeps_the_map() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("a", "1")?;
        h.insert("b", "2")?;
        h.insert("c", "3")?;
        let before = h.clone();
        assert_eq!(h.insert("d", "4"), Err(HeadersFull));
        assert_eq!(h, before);
        h.insert("a", "9")?;
        assert_eq!(h.get_str("a"), Some("9"));
        Ok(())
    }

    #[test]
    fn a_full_arena_refuses_an_entry_and_keeps_the_map() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("key", [b'v'; 20])?;
        let before = h.clone();
        // 3 + 20 bytes used, 9 free: the key and value below need 3 + 7 = 10.
        assert_eq!(h.insert("two", [b'w'; 7]), Err(HeadersFull));
        assert_eq!(h, before);
        h.insert("two", [b'w'; 6])?;
        assert_eq!(h.get("two"), Some([b'w'; 6].as_slice()));
        Ok(())
    }

    #[test]
    fn a_replacement_may_use_the_room_of_the_value_it_replaces() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("key", [b'v'; 29])?;
        h.insert("key", [b'w'; 29])?;
        assert_eq!(h.get("key"), Some([b'w'; 29].as_slice()));
        assert_eq!(h.insert("key", [b'x'; 30]), Err(HeadersFull));
        assert_eq!(h.get("key"), Some([b'w'; 29].as_slice()));
        Ok(())
    }

    #[test]
    fn remove_frees_the_room_and_keeps_the_other_entries() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("aa", "11")?;
        h.insert("bb", "22")?;
        h.insert("cc", "33")?;
        assert!(h.remove("BB"));
        assert!(!h.remove("bb"));
        assert!(!h.contains("bb"));
        assert_eq!(h.get_str("aa"), Some("11"));
        assert_eq!(h.get_str("cc"), Some("33"));
        h.insert("dd", "44")?;
        assert_eq!(h.len(), 3);
        assert_eq!(h.get_str("dd"), Some("44"));
        Ok(())
    }

    #[test]
    fn typed_accessors_return_values() -> Result<(), HeadersFull> {
        let mut h = FixedHeaders::<4, 96>::new();
        h.insert("Content-Type", "application/json")?;
        h.insert("Correlation-Id", "abc")?;
        h.insert("Reply-To", "inbox")?;
        h.insert("Message-Id", "m1")?;
        assert_eq!(h.content_type(), Some("application/json"));
        assert_eq!(h.correlation_id(), Some("abc"));
        assert_eq!(h.reply_to(), Some("inbox"));
        assert_eq!(h.message_id(), Some("m1"));
        Ok(())
    }

    #[test]
    fn a_value_that_is_not_utf8_reads_as_bytes_only() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("Content-Type", [0xff, 0xfe])?;
        assert_eq!(h.content_type(), None);
        assert_eq!(h.get("content-type"), Some([0xff, 0xfe].as_slice()));
        Ok(())
    }

    #[test]
    fn iter_yields_normalized_keys_in_insertion_order() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("Foo", "1")?;
        h.insert("Bar", "2")?;
        let pairs: Vec<_> = h.iter().collect();
        assert_eq!(pairs, [("foo", b"1".as_slice()), ("bar", b"2".as_slice())]);
        Ok(())
    }

    #[test]
    fn equality_ignores_insertion_order() -> Result<(), HeadersFull> {
        let mut a = Small::new();
        a.insert("x-a", "1")?;
        a.insert("x-b", "2")?;
        let mut b = Small::new();
        b.insert("X-B", "2")?;
        b.insert("x-a", "1")?;
        assert_eq!(a, b);
        b.insert("x-a", "3")?;
        assert_ne!(a, b);
        Ok(())
    }

    #[test]
    fn debug_lists_the_entries() -> Result<(), HeadersFull> {
        let mut h = Small::new();
        h.insert("a", "1")?;
        assert_eq!(format!("{h:?}"), r#"{"a": [49]}"#);
        Ok(())
    }

    #[test]
    fn overwrite_with_merges_key_by_key() -> Result<(), HeadersFull> {
        let mut base = Small::new();
        base.insert("tenant", "acme")?;
        base.insert("x", "old")?;
        let mut call = Small::new();
        call.insert("x", "new")?;
        base.overwrite_with(&call)?;
        assert_eq!(base.get_str("tenant"), Some("acme"));
        assert_eq!(base.get_str("x"), Some("new"));
        Ok(())
    }

    #[test]
    fn overwrite_with_that_does_not_fit_keeps_the_map() -> Result<(), HeadersFull> {
        let mut base = Small::new();
        base.insert("a", "1")?;
        base.insert("b", "2")?;
        let mut call = Small::new();
        call.insert("c", "3")?;
        call.insert("d", "4")?;
        let before = base.clone();
        assert_eq!(base.overwrite_with(&call), Err(HeadersFull));
        assert_eq!(base, before);
        Ok(())
    }

    #[test]
    fn the_full_error_says_what_is_full() {
        assert_eq!(
            format!("{HeadersFull}"),
            "the header map is full: no free entry or no room for the bytes"
        );
    }
}
