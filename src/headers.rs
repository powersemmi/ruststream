//! Message headers with typed accessors for well-known fields.

use std::{borrow::Cow, collections::HashMap};

use bytes::Bytes;

/// Case-insensitive map of broker-message headers.
///
/// Keys are normalized to ASCII lowercase on insertion. Values are stored as `Bytes` to support
/// arbitrary binary metadata. Typed accessors are provided for well-known fields commonly carried
/// by message brokers; unknown headers are read through [`Headers::get`].
///
/// # Examples
///
/// ```
/// use ruststream::Headers;
///
/// let mut h = Headers::new();
/// h.insert("Content-Type", "application/json");
/// h.insert("X-Tenant-Id", "acme");
///
/// assert_eq!(h.content_type(), Some("application/json"));
/// assert_eq!(h.get("x-tenant-id"), Some(b"acme".as_slice()));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers {
    inner: HashMap<String, Bytes>,
}

impl Headers {
    /// Returns an empty header map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an empty header map with capacity for at least `cap` entries.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(cap),
        }
    }

    /// Inserts a header value, returning the previous value under that key if any.
    ///
    /// The key is normalized to ASCII lowercase.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Option<Bytes> {
        let key = normalize_owned(name.into());
        self.inner.insert(key, value.into())
    }

    /// Returns the raw bytes of a header value, or `None` if the header is absent.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let key = normalize_borrowed(name);
        self.inner.get(key.as_ref()).map(Bytes::as_ref)
    }

    /// Returns the value of a header decoded as UTF-8, or `None` if absent or not valid UTF-8.
    #[must_use]
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|raw| std::str::from_utf8(raw).ok())
    }

    /// Removes a header by name and returns its previous value, if any.
    pub fn remove(&mut self, name: &str) -> Option<Bytes> {
        let key = normalize_borrowed(name);
        self.inner.remove(key.as_ref())
    }

    /// Returns `true` if the given header is present.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        let key = normalize_borrowed(name);
        self.inner.contains_key(key.as_ref())
    }

    /// Returns the number of headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if no headers are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterates over `(name, value)` pairs. Names are returned in their normalized lowercase form.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.inner.iter().map(|(k, v)| (k.as_str(), v.as_ref()))
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
        // Nothing to keep means nothing to merge: the map moves in whole, which is what a publish
        // without a base has always done.
        if self.inner.is_empty() {
            *self = other;
        } else {
            self.inner.extend(other.inner);
        }
    }
}

impl<K, V> FromIterator<(K, V)> for Headers
where
    K: Into<String>,
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

fn normalize_owned(mut s: String) -> String {
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        s.make_ascii_lowercase();
    }
    s
}

fn normalize_borrowed(s: &str) -> Cow<'_, str> {
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(s.to_ascii_lowercase())
    } else {
        Cow::Borrowed(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_case_insensitive() {
        let mut h = Headers::new();
        h.insert("Content-Type", "application/json");
        assert_eq!(h.get("content-type"), Some(b"application/json".as_slice()));
        assert_eq!(h.get("CONTENT-TYPE"), Some(b"application/json".as_slice()));
        assert_eq!(h.get_str("Content-Type"), Some("application/json"));
    }

    #[test]
    fn typed_accessors_return_values() {
        let mut h = Headers::new();
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
        let mut h = Headers::new();
        h.insert("Content-Type", Bytes::from_static(&[0xff, 0xfe]));
        assert_eq!(h.content_type(), None);
        assert_eq!(h.get("content-type"), Some([0xff, 0xfe].as_slice()));
    }

    #[test]
    fn remove_and_contains() {
        let mut h = Headers::new();
        h.insert("X-Tenant", "acme");
        assert!(h.contains("x-tenant"));
        assert_eq!(h.remove("X-TENANT"), Some(Bytes::from_static(b"acme")));
        assert!(!h.contains("x-tenant"));
    }

    #[test]
    fn collect_via_from_iterator() {
        let h: Headers = [("Foo", "1"), ("Bar", "2")].into_iter().collect();
        assert_eq!(h.len(), 2);
        assert_eq!(h.get_str("foo"), Some("1"));
        assert_eq!(h.get_str("bar"), Some("2"));
    }

    #[test]
    fn overwrite_with_upserts_key_by_key() {
        let mut base: Headers = [("tenant", "acme"), ("x-trace", "handle")]
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
        let mut call = Headers::new();
        call.insert("x-trace", "call");
        let mut base = Headers::new();
        base.overwrite_with(call);
        assert_eq!(base.get_str("x-trace"), Some("call"));

        let mut kept = Headers::new();
        kept.insert("tenant", "acme");
        kept.overwrite_with(Headers::new());
        assert_eq!(kept.get_str("tenant"), Some("acme"));
    }

    #[test]
    fn iter_yields_normalized_keys() {
        let mut h = Headers::new();
        h.insert("Foo", "1");
        let pairs: Vec<_> = h.iter().collect();
        assert_eq!(pairs, vec![("foo", b"1".as_slice())]);
    }
}
