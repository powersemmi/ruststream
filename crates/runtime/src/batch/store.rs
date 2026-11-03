//! Where a batch lives while it is handled: a `Vec` with `alloc`, a fixed buffer without.
//!
//! The batch size a mount names is a number the broker's slot respects. Without `alloc` the
//! runtime's own storage for the batch (the taken deliveries, the decoded values, the outcomes)
//! is sized by [`BATCH_CAPACITY`], and `.batch::<N>()` is a compile error past it.

#[cfg(ruststream_alloc)]
use alloc::vec::{Drain, Vec};
#[cfg(not(ruststream_alloc))]
use core::num::NonZeroUsize;
#[cfg(not(ruststream_alloc))]
use core::ops::Deref;
#[cfg(not(ruststream_alloc))]
use heapless::vec::Drain;

#[cfg(not(ruststream_alloc))]
use crate::batch::{BatchResult, IntoBatchResult};
#[cfg(not(ruststream_alloc))]
use crate::handler::HandlerOutcome;

/// The most messages one batch holds on a build without `alloc`.
pub const BATCH_CAPACITY: usize = 16;

/// The runtime's storage of one batch's elements.
#[cfg(ruststream_alloc)]
pub type BatchVec<T> = Vec<T>;

/// The runtime's storage of one batch's elements.
#[cfg(not(ruststream_alloc))]
pub type BatchVec<T> = heapless::Vec<T, BATCH_CAPACITY>;

/// What draining a [`BatchVec`] yields: its elements, taken out in place.
#[cfg(ruststream_alloc)]
pub(crate) type BatchDrain<'vec, T> = Drain<'vec, T>;

/// What draining a [`BatchVec`] yields: its elements, taken out in place.
#[cfg(not(ruststream_alloc))]
pub(crate) type BatchDrain<'vec, T> = Drain<'vec, T, usize>;

/// Building a batch's storage and filling it.
pub(crate) trait Gather<T> {
    /// Storage for a batch of up to `size` elements.
    fn with_room(size: usize) -> Self;

    /// Adds the next element.
    fn put(&mut self, item: T);
}

#[cfg(ruststream_alloc)]
impl<T> Gather<T> for Vec<T> {
    fn with_room(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn put(&mut self, item: T) {
        self.push(item);
    }
}

#[cfg(not(ruststream_alloc))]
impl<T> Gather<T> for heapless::Vec<T, BATCH_CAPACITY> {
    fn with_room(_size: usize) -> Self {
        Self::new()
    }

    fn put(&mut self, item: T) {
        // Why a panic: the slot holds at most the mount's batch size, which `.batch::<N>()`
        // keeps within the capacity at compile time; a batch past it is a slot that ignores the
        // size it was opened at, and dropping its messages would lose them.
        assert!(
            self.push(item).is_ok(),
            "a batch holds at most BATCH_CAPACITY messages"
        );
    }
}

/// What a batch handler returns for its elements without `alloc`: one entry per element, at most `N`.
///
/// The entries are outcomes (`Outcomes<HandlerOutcome, N>`), or the replies of a handler mounted
/// with `reply` (`Outcomes<Reply, N>`). With `alloc` the same position is a `Vec`.
#[cfg(not(ruststream_alloc))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcomes<T = HandlerOutcome, const N: usize = BATCH_CAPACITY>(heapless::Vec<T, N>);

#[cfg(not(ruststream_alloc))]
impl<T, const N: usize> Outcomes<T, N> {
    /// No entries yet.
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                N <= BATCH_CAPACITY,
                "a batch holds at most BATCH_CAPACITY messages"
            );
        };
        Self(heapless::Vec::new())
    }

    /// Adds the entry of the next element.
    ///
    /// # Errors
    ///
    /// Gives the entry back when `N` are already held.
    pub fn push(&mut self, entry: T) -> Result<(), T> {
        self.0.push(entry)
    }

    /// The entries, in the order of the elements.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
}

#[cfg(not(ruststream_alloc))]
impl<T, const N: usize> Default for Outcomes<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(ruststream_alloc))]
impl<T, const N: usize> Deref for Outcomes<T, N> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.0
    }
}

#[cfg(not(ruststream_alloc))]
impl<T, const N: usize> IntoIterator for Outcomes<T, N> {
    type Item = T;
    type IntoIter = <heapless::Vec<T, N> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Collects the entries of a batch.
///
/// # Panics
///
/// When the iterator yields more than `N` entries: a batch never holds more than the size its
/// mount named, so a longer iterator is a handler that did not follow its slice.
#[cfg(not(ruststream_alloc))]
impl<T, const N: usize> FromIterator<T> for Outcomes<T, N> {
    fn from_iter<Entries: IntoIterator<Item = T>>(entries: Entries) -> Self {
        let mut held = Self::new();
        for entry in entries {
            assert!(held.push(entry).is_ok(), "a batch holds at most N entries");
        }
        held
    }
}

/// A batch handler that returns its replies hands them to the runtime in its own storage, in the
/// order of the batch.
#[cfg(not(ruststream_alloc))]
impl<T, const N: usize> From<Outcomes<T, N>> for BatchVec<T> {
    fn from(replies: Outcomes<T, N>) -> Self {
        // Never full before the last entry: `Outcomes` holds at most `BATCH_CAPACITY`.
        replies.into_iter().collect()
    }
}

/// A batch handler that returns its entries settles each element by its own.
#[cfg(not(ruststream_alloc))]
impl<const N: usize> IntoBatchResult for Outcomes<HandlerOutcome, N> {
    fn into_batch_result(self) -> BatchResult {
        BatchResult::PerElement(self.into_iter().collect())
    }
}

/// The size of a batch a mount names: `BatchSize<N>` has a [`FitsBatch`] impl exactly for the
/// sizes the runtime's storage holds.
#[cfg(not(ruststream_alloc))]
#[derive(Debug, Clone, Copy)]
pub struct BatchSize<const SIZE: usize>;

/// A batch size the runtime's fixed storage holds: from one message up to [`BATCH_CAPACITY`].
#[cfg(not(ruststream_alloc))]
#[diagnostic::on_unimplemented(
    message = "a batch of this size does not fit the runtime's storage: `{Self}`",
    label = "the batch size is 0 or past BATCH_CAPACITY (16)",
    note = "a build without `alloc` holds a batch in fixed storage; name a size from 1 to 16 in \
            `.batch::<N>()`"
)]
pub trait FitsBatch {
    /// The size as the broker's slot is asked for it.
    const SIZE: NonZeroUsize;
}

#[cfg(not(ruststream_alloc))]
macro_rules! fits_batch {
    ($($size:literal)*) => {$(
        impl FitsBatch for BatchSize<$size> {
            const SIZE: NonZeroUsize = NonZeroUsize::new($size).unwrap();
        }
    )*};
}

#[cfg(not(ruststream_alloc))]
fits_batch!(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16);

// The list above ends where the storage does.
#[cfg(not(ruststream_alloc))]
const _: () = assert!(
    <BatchSize<BATCH_CAPACITY> as FitsBatch>::SIZE.get() == BATCH_CAPACITY,
    "the sizes a mount may name end at BATCH_CAPACITY"
);

/// The batch outcome the macros name: a `Vec` with `alloc`, `Outcomes` without.
#[cfg(ruststream_alloc)]
pub type BatchOutcomes<T = crate::HandlerOutcome> = Vec<T>;

/// The batch outcome the macros name: a `Vec` with `alloc`, `Outcomes` without.
#[cfg(not(ruststream_alloc))]
pub type BatchOutcomes<T = HandlerOutcome> = Outcomes<T, BATCH_CAPACITY>;

#[cfg(test)]
#[cfg(not(ruststream_alloc))]
mod fixed_tests {
    use super::Outcomes;
    use crate::batch::{BatchResult, BatchVec, IntoBatchResult};
    use crate::handler::HandlerOutcome;

    #[test]
    fn a_batch_of_four_settles_each_element_by_its_own_entry() {
        let outcomes: Outcomes<HandlerOutcome, 4> = [
            HandlerOutcome::ack(),
            HandlerOutcome::drop(),
            HandlerOutcome::ack(),
            HandlerOutcome::retry(),
        ]
        .into_iter()
        .collect();

        let BatchResult::PerElement(entries) = outcomes.into_batch_result() else {
            panic!("a per-element batch result");
        };

        assert_eq!(entries.len(), 4);
        assert!(entries[0].is_ack());
        assert!(!entries[1].is_ack());
    }

    #[test]
    fn the_replies_of_a_batch_keep_their_order_in_the_runtimes_storage() {
        let replies: Outcomes<u8, 3> = [7, 8, 9].into_iter().collect();

        let stored: BatchVec<u8> = replies.into();

        assert_eq!(stored.as_slice(), [7, 8, 9]);
    }
}
