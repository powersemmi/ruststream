//! The [`Extensions`] per-delivery type-map.

use std::any::{Any, TypeId};
use std::collections::HashMap;

/// A per-delivery type-map: at most one value per type, scoped to a single delivery.
///
/// Unlike the app-level `State` (shared, set once at build), an `Extensions` map is created fresh
/// for each delivery and dropped when the handler returns, so one delivery's values never leak into
/// the next. A broker contributes typed per-delivery data through
/// [`IncomingMessage::extensions`](crate::IncomingMessage::extensions) (native delivery metadata,
/// a commit token, a reply-to handle) without going through the byte-only headers; middleware may
/// write entries for downstream handlers (via
/// [`Context::insert`](crate::runtime::Context::insert)); and the values are reachable from the
/// publish path so a transactional publisher can read a broker-supplied commit token at commit
/// time.
///
/// # Examples
///
/// ```
/// use ruststream::Extensions;
///
/// # fn run() -> Option<()> {
/// let mut ext = Extensions::new();
/// ext.insert(7u32);
/// assert_eq!(*ext.get::<u32>()?, 7);
/// # Some(())
/// # }
/// # run().expect("value was inserted");
/// ```
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    /// Creates an empty map.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Extensions;
    ///
    /// let ext = Extensions::new();
    /// assert!(ext.is_empty());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts `value`, replacing any previous value of the same type.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Extensions;
    ///
    /// # fn run() -> Option<()> {
    /// let mut ext = Extensions::new();
    /// ext.insert("first");
    /// ext.insert("second");
    /// assert_eq!(*ext.get::<&str>()?, "second");
    /// # Some(())
    /// # }
    /// # run().expect("value was inserted");
    /// ```
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Returns the stored value of type `T`, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Extensions;
    ///
    /// let mut ext = Extensions::new();
    /// ext.insert(42u64);
    /// assert_eq!(ext.get::<u64>(), Some(&42));
    /// assert_eq!(ext.get::<i8>(), None);
    /// ```
    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Whether the map holds no entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Extensions;
    ///
    /// let mut ext = Extensions::new();
    /// assert!(ext.is_empty());
    /// ext.insert(1u8);
    /// assert!(!ext.is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("entries", &self.map.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::Extensions;

    #[test]
    fn one_value_per_type_and_distinct_types_coexist() {
        let mut ext = Extensions::new();
        assert!(ext.is_empty());
        ext.insert(1u32);
        ext.insert(2u32);
        assert_eq!(ext.get::<u32>(), Some(&2));
        ext.insert("tag");
        assert_eq!(ext.get::<&str>(), Some(&"tag"));
        assert_eq!(ext.get::<i64>(), None);
        assert!(!ext.is_empty());
    }

    #[test]
    fn debug_reports_entry_count() {
        let mut ext = Extensions::new();
        ext.insert(1u8);
        assert!(format!("{ext:?}").contains("entries: 1"));
    }
}
