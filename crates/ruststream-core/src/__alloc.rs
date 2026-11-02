//! The heap dependencies of the family's heap variant, which the other crates reach through the
//! contract.
//!
//! The contract's `alloc` feature brings them, so a crate that needs them in its heap variant
//! names no optional dependency of its own: it follows the contract's variant, whichever crate
//! turned the feature on. A build without `alloc` links none of them.

#[cfg(any(feature = "alloc", feature = "testing"))]
pub use bytes;
#[cfg(any(feature = "alloc", feature = "testing"))]
pub use bytes_utils;
#[cfg(feature = "alloc")]
pub use tracing;
