//! The harness's integration suite, one test binary: each module is named for the module of the
//! crate it covers, and is gated on the runtime it drives. The compio tests keep a binary each.

// A batch handler with an `Out` slot and a store of retries nests the app's futures past the default
// recursion limit of 128 (#684 lowers the depth of the `Out` forms); the app of a service sets the
// same attribute.
#![recursion_limit = "256"]

#[cfg(all(feature = "tokio", feature = "embassy", feature = "json"))]
mod both_runtimes;
#[cfg(all(feature = "embassy", feature = "json"))]
mod embedded;
#[cfg(all(feature = "tokio", feature = "json"))]
mod handle;
#[cfg(all(feature = "tokio", feature = "json"))]
mod harness;
