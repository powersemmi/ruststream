//! The heap dependencies of the family's heap variant, which the other crates reach through the
//! contract.
//!
//! The contract's `alloc` feature brings them (`tracing` comes with its own feature, which asks for `alloc`), so a crate that needs them in its heap variant
//! names no optional dependency of its own: it follows the contract's variant, whichever crate
//! turned the feature on. A build without `alloc` links none of them.

#[cfg(any(feature = "alloc", feature = "testing"))]
pub use bytes;
#[cfg(any(feature = "alloc", feature = "testing"))]
pub use bytes_utils;
#[cfg(all(feature = "alloc", target_has_atomic = "ptr"))]
pub use spin;
#[cfg(all(feature = "tracing", target_has_atomic = "ptr"))]
pub use tracing;
