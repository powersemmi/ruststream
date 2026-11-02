//! The conformance suite of [RustStream](https://docs.rs/ruststream): the contract checks a broker
//! crate runs against its in-process transport and against a running broker.
//!
//! Broker crates use it through the `ruststream` facade, as
//! [`ruststream::conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/), whose
//! overview teaches it.

// The conformance suites drive a broker's cold path, whose futures carry no Send.
#![allow(clippy::future_not_send)]

#[cfg(ruststream_alloc)]
pub mod capabilities;
pub mod harness;
pub mod helpers;
#[cfg(ruststream_alloc)]
pub mod in_process;
#[cfg(ruststream_alloc)]
pub mod lifecycle;
/// The refused-publish declaration of the local family's checks; the ladder of the `Send`
/// family is in the build with the framework's `alloc`.
#[cfg(not(ruststream_alloc))]
pub mod lifecycle {
    pub use crate::unsent::Unsent;
}
#[cfg(ruststream_alloc)]
pub mod message_shape;
#[cfg(ruststream_alloc)]
pub mod retry;
#[cfg(ruststream_alloc)]
pub mod settlement;
mod unsent;
