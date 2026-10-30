//! What the metric layers record under one destination, built the first time the name is seen and
//! reused by every later message.
//!
//! A destination reaches a layer as a borrowed `&str`, while an OpenTelemetry attribute owns its
//! string and a Prometheus series is found by hashing its labels. Building either per message is
//! an allocation per message; built here once per name, a message pays one lookup.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;

/// The distinct destinations a layer keeps entries for, the OpenTelemetry SDK's default
/// cardinality limit per instrument. A name past it is still recorded, with an entry built for
/// that one message.
pub(crate) const CAPACITY: usize = 2000;

/// The entries of one layer, one per destination name.
///
/// Entries live in an append-only vector, so a reference to one stays valid for as long as the
/// cache does and can be held across the publish it describes. The index from name to entry is
/// replaced whole when a name is added, so a lookup takes no lock: it loads the current index
/// (`arc-swap`'s thread-local debt slot, no shared counter) and hashes the name once.
pub(crate) struct ByName<Entry> {
    index: ArcSwap<HashMap<Box<str>, usize>>,
    entries: boxcar::Vec<Entry>,
    capacity: usize,
}

impl<Entry> ByName<Entry> {
    /// An empty cache keeping entries for at most `capacity` names.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            index: ArcSwap::from_pointee(HashMap::new()),
            entries: boxcar::Vec::new(),
            capacity,
        }
    }

    /// The entry of `name`, built by `build` the first time the name is seen.
    ///
    /// Two threads meeting a new name at once may both build it; one entry wins the index and the
    /// other stays unused, so a name costs at most one entry per thread that raced for it. Past
    /// the capacity, `build` runs for this call alone.
    pub(crate) fn get_or_build(
        &self,
        name: &str,
        build: impl FnOnce(&str) -> Entry,
    ) -> Recorded<'_, Entry> {
        if let Some(&slot) = self.index.load().get(name) {
            return Recorded::Kept(&self.entries[slot]);
        }
        if self.entries.count() >= self.capacity {
            return Recorded::Once(build(name));
        }
        let slot = self.entries.push(build(name));
        self.index.rcu(|index| {
            let mut next = HashMap::clone(index);
            next.insert(Box::from(name), slot);
            Arc::new(next)
        });
        Recorded::Kept(&self.entries[slot])
    }
}

/// An entry the cache keeps, or one built for a single message past its capacity.
pub(crate) enum Recorded<'a, Entry> {
    /// The entry the cache keeps for the name.
    Kept(&'a Entry),
    /// An entry built for this message alone.
    Once(Entry),
}

impl<Entry> Deref for Recorded<'_, Entry> {
    type Target = Entry;

    fn deref(&self) -> &Entry {
        match self {
            Self::Kept(entry) => entry,
            Self::Once(entry) => entry,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{ByName, Recorded};

    #[test]
    fn a_name_is_built_once_and_found_after() {
        let cache = ByName::new(4);
        let calls = Cell::new(0);
        let build = |name: &str| {
            calls.set(calls.get() + 1);
            name.len()
        };

        let first = cache.get_or_build("orders", build);
        assert!(matches!(first, Recorded::Kept(_)));
        assert_eq!(*first, 6);
        let again = cache.get_or_build("orders", build);
        assert!(matches!(again, Recorded::Kept(_)));
        assert_eq!(
            calls.get(),
            1,
            "a name seen before is found, not built again"
        );
        assert_eq!(*cache.get_or_build("invoices", build), 8);
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn a_name_past_the_capacity_is_built_for_each_message() {
        let cache = ByName::new(1);
        let calls = Cell::new(0);
        let build = |name: &str| {
            calls.set(calls.get() + 1);
            name.len()
        };

        assert!(matches!(
            cache.get_or_build("kept", build),
            Recorded::Kept(_)
        ));
        let past = cache.get_or_build("overflow", build);
        assert!(matches!(past, Recorded::Once(_)));
        assert_eq!(*past, 8);
        assert!(matches!(
            cache.get_or_build("overflow", build),
            Recorded::Once(_)
        ));
        assert_eq!(
            calls.get(),
            3,
            "a name past the capacity is built on every message"
        );
        assert!(
            matches!(cache.get_or_build("kept", build), Recorded::Kept(_)),
            "a name kept before the capacity filled is still found",
        );
    }
}
