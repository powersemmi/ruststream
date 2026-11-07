//! The harness's integration suite, one test binary: each module is named for the module of the
//! crate it covers, and is gated on the runtime it drives. The compio tests keep a binary each.
//!
//! The suite builds at the compiler's default recursion limit: the embedded apps of its `Out`
//! forms (a slot beside a store of retries, a batch handler with a slot) fail this build where a
//! change nests their futures deeper than a service's app may.

#[cfg(all(feature = "tokio", feature = "embassy", feature = "json"))]
mod both_runtimes;
#[cfg(all(feature = "embassy", feature = "json"))]
mod embedded;
#[cfg(all(feature = "tokio", feature = "json"))]
mod handle;
#[cfg(all(feature = "tokio", feature = "json"))]
mod harness;
