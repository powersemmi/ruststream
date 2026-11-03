//! Message headers: the map a message carries, in the form the build selects.
//!
//! With `alloc` the headers are a [`HeaderMap`], whose entries are shared buffers on the heap.
//! Without it they are a [`FixedHeaders`], whose entries and bytes are fixed at compile time and
//! whose `insert` returns an error when the map is full. [`Headers`] names the active one, so a
//! message, a publish builder and the typed headers are written once for both builds.

#[cfg(not(feature = "alloc"))]
mod fixed;
#[cfg(feature = "alloc")]
mod map;

#[cfg(not(feature = "alloc"))]
pub use fixed::{FixedHeaders, HeadersFull};
#[cfg(feature = "alloc")]
pub use map::HeaderMap;

/// The headers of a message in this build: a [`HeaderMap`].
#[cfg(feature = "alloc")]
pub type Headers = HeaderMap;

/// The number of entries the headers of a message hold in this build.
#[cfg(not(feature = "alloc"))]
pub const HEADER_ENTRIES: usize = 8;

/// The bytes, keys and values together, the headers of a message hold in this build.
#[cfg(not(feature = "alloc"))]
pub const HEADER_BYTES: usize = 256;

/// The headers of a message in this build: a [`FixedHeaders`] of [`HEADER_ENTRIES`] entries and
/// [`HEADER_BYTES`] bytes.
#[cfg(not(feature = "alloc"))]
pub type Headers = FixedHeaders<HEADER_ENTRIES, HEADER_BYTES>;
