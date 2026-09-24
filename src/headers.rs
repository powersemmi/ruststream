//! Message headers with typed accessors for well-known fields.

use std::{fmt, mem};

use bytes::Bytes;
use bytes_utils::Str;

/// Case-insensitive map of broker-message headers.
///
/// Keys are normalized to ASCII lowercase on insertion. Both halves of an entry are shared
/// buffers, [`Str`] and [`Bytes`], so cloning a map is a reference count per entry and a broker
/// hands over a key its own read buffer already holds. Values are bytes to support arbitrary
/// binary metadata. Typed accessors are provided for well-known fields commonly carried by
/// message brokers; unknown headers are read through [`HeaderMap::get`], or through
/// [`HeaderMap::get_shared`] where the value is to outlive the borrow.
///
/// Two maps are equal when they hold the same entries, whatever order the entries were inserted
/// in. A lookup scans the entries, so its cost grows linearly with the number of headers; a
/// broker's frame limit bounds that number, and over the handful of headers a message carries the
/// scan is cheaper than hashing the name.
///
/// # Examples
///
/// ```
/// use ruststream::HeaderMap;
///
/// let mut h = HeaderMap::new();
/// h.insert("Content-Type", "application/json");
/// h.insert("X-Tenant-Id", "acme");
///
/// assert_eq!(h.content_type(), Some("application/json"));
/// assert_eq!(h.get("x-tenant-id"), Some(b"acme".as_slice()));
///
/// let mut same = HeaderMap::new();
/// same.insert("x-tenant-id", "acme");
/// same.insert("content-type", "application/json");
/// assert_eq!(h, same);
/// ```
// A list rather than a hash table: an empty map allocates nothing, the first insert allocates one
// block instead of a table, and a lookup with capitals in the name compares without lowercasing a
// copy of it.
#[derive(Clone, Default)]
pub struct HeaderMap {
    // `None` until the first insert: the drop glue of an empty map is then one branch, where an
    // empty `Vec` still calls into its element drop and its deallocation check.
    inner: Option<Vec<(Str, Bytes)>>,
}

impl fmt::Debug for HeaderMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.entries().iter().map(|(k, v)| (k, v)))
            .finish()
    }
}

/// Equal when both hold the same entries, in whatever order they were inserted.
impl PartialEq for HeaderMap {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.entries().iter().all(|(k, v)| {
                other
                    .position(k)
                    .is_some_and(|i| other.entries()[i].1 == *v)
            })
    }
}

impl Eq for HeaderMap {}

impl HeaderMap {
    /// Returns an empty header map.
    #[must_use]
    pub const fn new() -> Self {
        Self { inner: None }
    }

    /// Returns an empty header map with capacity for at least `cap` entries.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: (cap > 0).then(|| Vec::with_capacity(cap)),
        }
    }

    /// Inserts a header value, returning the previous value under that key if any.
    ///
    /// The key is normalized to ASCII lowercase. A constant key is written
    /// `Str::from_static("content-type")`, which costs nothing; a `String` moves in without a
    /// copy; a `&str` is copied once, and a key that is not already lowercase is copied again.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{HeaderMap, Str};
    ///
    /// let mut headers = HeaderMap::new();
    /// headers.insert(Str::from_static("content-type"), "application/json");
    /// headers.insert(format!("x-tenant-{}", 7), "acme");
    ///
    /// assert_eq!(headers.content_type(), Some("application/json"));
    /// assert_eq!(headers.get_str("x-tenant-7"), Some("acme"));
    /// ```
    pub fn insert(&mut self, name: impl Into<Str>, value: impl Into<Bytes>) -> Option<Bytes> {
        let key = normalize_owned(name.into());
        let value = value.into();
        if let Some(index) = self.position(&key) {
            return Some(mem::replace(&mut self.entries_mut()[index].1, value));
        }
        self.entries_mut().push((key, value));
        None
    }

    fn entries(&self) -> &[(Str, Bytes)] {
        self.inner.as_deref().unwrap_or_default()
    }

    fn entries_mut(&mut self) -> &mut Vec<(Str, Bytes)> {
        self.inner.get_or_insert_with(Vec::new)
    }

    /// Where the entry named `name` sits, compared without regard to case: stored keys are
    /// lowercase, so this is the lookup a lowercased copy of `name` would make, without the copy.
    fn position(&self, name: &str) -> Option<usize> {
        self.entries().iter().position(|(k, _)| {
            k.len() == name.len() && k.as_bytes().eq_ignore_ascii_case(name.as_bytes())
        })
    }

    /// Returns the raw bytes of a header value, or `None` if the header is absent.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.position(name)
            .map(|index| self.entries()[index].1.as_ref())
    }

    /// Returns a header value as the shared buffer the map stores, or `None` if the header is
    /// absent.
    ///
    /// [`get`](Self::get) borrows the bytes for as long as the map lives; this hands over a
    /// counted handle on them instead, which outlives the borrow and costs a reference count
    /// rather than a copy. It is how a value becomes something that travels: a reply address read
    /// off a delivery becomes an outgoing destination through `Str::try_from`, a UTF-8 check over
    /// the same bytes that yields a [`Str`].
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{HeaderMap, Str};
    ///
    /// let mut headers = HeaderMap::new();
    /// headers.insert("Reply-To", "replies.inbox");
    ///
    /// let shared = headers.get_shared("reply-to").ok_or("no reply address")?;
    /// let destination = Str::try_from(shared)?;
    /// assert_eq!(&*destination, "replies.inbox");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn get_shared(&self, name: &str) -> Option<Bytes> {
        self.position(name)
            .map(|index| self.entries()[index].1.clone())
    }

    /// Returns the value of a header decoded as UTF-8, or `None` if absent or not valid UTF-8.
    #[must_use]
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|raw| std::str::from_utf8(raw).ok())
    }

    /// Removes a header by name and returns its previous value, if any.
    pub fn remove(&mut self, name: &str) -> Option<Bytes> {
        let index = self.position(name)?;
        Some(self.entries_mut().remove(index).1)
    }

    /// Returns `true` if the given header is present.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.position(name).is_some()
    }

    /// Returns the number of headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries().len()
    }

    /// Returns `true` if no headers are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries().is_empty()
    }

    /// Iterates over `(name, value)` pairs. Names are returned in their normalized lowercase form.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.entries().iter().map(|(k, v)| (&**k, v.as_ref()))
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
    /// The publish builder's merge: a sink's base map takes the call site's map on top of it, key
    /// by key. Keys arrive already normalized (every insertion path lowercases them), so the
    /// entries move straight across.
    pub(crate) fn overwrite_with(&mut self, other: Self) {
        // Nothing to keep means nothing to merge: the map moves in whole.
        if self.is_empty() {
            *self = other;
        } else {
            for (key, value) in other.inner.into_iter().flatten() {
                match self.position(&key) {
                    Some(index) => self.entries_mut()[index].1 = value,
                    None => self.entries_mut().push((key, value)),
                }
            }
        }
    }
}

impl<K, V> FromIterator<(K, V)> for HeaderMap
where
    K: Into<Str>,
    V: Into<Bytes>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut headers = Self::with_capacity(lower);
        for (k, v) in iter {
            headers.insert(k, v);
        }
        headers
    }
}

/// The key a shared buffer carries: kept as it arrived when it is already lowercase, which is
/// what every framework-issued key and every wire format that lowercases its own headers hands
/// in. A key with capitals in it is copied, since a shared buffer cannot be lowercased in place.
fn normalize_owned(s: Str) -> Str {
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        Str::from(s.to_ascii_lowercase())
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_ignores_insertion_order() {
        let mut a = HeaderMap::new();
        a.insert("x-a", "1");
        a.insert("x-b", "2");
        let mut b = HeaderMap::new();
        b.insert("X-B", "2");
        b.insert("x-a", "1");
        assert_eq!(a, b);
        b.insert("x-a", "3");
        assert_ne!(a, b);
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn insert_and_get_case_insensitive() {
        let mut h = HeaderMap::new();
        h.insert("Content-Type", "application/json");
        assert_eq!(h.get("content-type"), Some(b"application/json".as_slice()));
        assert_eq!(h.get("CONTENT-TYPE"), Some(b"application/json".as_slice()));
        assert_eq!(h.get_str("Content-Type"), Some("application/json"));
    }

    #[test]
    fn typed_accessors_return_values() {
        let mut h = HeaderMap::new();
        h.insert("Content-Type", "application/json");
        h.insert("Correlation-Id", "abc-123");
        h.insert("Reply-To", "responses.inbox");
        h.insert("Message-Id", "msg-1");

        assert_eq!(h.content_type(), Some("application/json"));
        assert_eq!(h.correlation_id(), Some("abc-123"));
        assert_eq!(h.reply_to(), Some("responses.inbox"));
        assert_eq!(h.message_id(), Some("msg-1"));
    }

    #[test]
    fn typed_accessor_returns_none_for_non_utf8() {
        let mut h = HeaderMap::new();
        h.insert("Content-Type", Bytes::from_static(&[0xff, 0xfe]));
        assert_eq!(h.content_type(), None);
        assert_eq!(h.get("content-type"), Some([0xff, 0xfe].as_slice()));
    }

    #[test]
    fn remove_and_contains() {
        let mut h = HeaderMap::new();
        h.insert("X-Tenant", "acme");
        assert!(h.contains("x-tenant"));
        assert_eq!(h.remove("X-TENANT"), Some(Bytes::from_static(b"acme")));
        assert!(!h.contains("x-tenant"));
    }

    #[test]
    fn collect_via_from_iterator() {
        let h: HeaderMap = [("Foo", "1"), ("Bar", "2")].into_iter().collect();
        assert_eq!(h.len(), 2);
        assert_eq!(h.get_str("foo"), Some("1"));
        assert_eq!(h.get_str("bar"), Some("2"));
    }

    #[test]
    fn overwrite_with_upserts_key_by_key() {
        let mut base: HeaderMap = [("tenant", "acme"), ("x-trace", "handle")]
            .into_iter()
            .collect();
        base.overwrite_with(
            [("x-trace", "call"), ("x-request-id", "r-1")]
                .into_iter()
                .collect(),
        );

        assert_eq!(base.get_str("x-trace"), Some("call"));
        assert_eq!(base.get_str("tenant"), Some("acme"));
        assert_eq!(base.get_str("x-request-id"), Some("r-1"));
        assert_eq!(base.len(), 3);
    }

    #[test]
    fn overwrite_with_moves_the_whole_map_over_an_empty_one() {
        let mut call = HeaderMap::new();
        call.insert("x-trace", "call");
        let mut base = HeaderMap::new();
        base.overwrite_with(call);
        assert_eq!(base.get_str("x-trace"), Some("call"));

        let mut kept = HeaderMap::new();
        kept.insert("tenant", "acme");
        kept.overwrite_with(HeaderMap::new());
        assert_eq!(kept.get_str("tenant"), Some("acme"));
    }

    #[test]
    fn iter_yields_normalized_keys() {
        let mut h = HeaderMap::new();
        h.insert("Foo", "1");
        let pairs: Vec<_> = h.iter().collect();
        assert_eq!(pairs, vec![("foo", b"1".as_slice())]);
    }
}
