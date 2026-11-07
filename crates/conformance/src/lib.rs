//! The conformance suite of [RustStream](https://docs.rs/ruststream): the contract checks a broker
//! crate runs against its in-process transport and against a running broker.
//!
//! Broker crates use it through the `ruststream` facade, as
//! [`ruststream::conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/), whose
//! overview teaches it.

// The conformance suites drive a broker's cold path, whose futures carry no Send.
#![allow(clippy::future_not_send)]

#[cfg(ruststream_send_family)]
pub mod capabilities;
pub mod harness;
pub mod helpers;
#[cfg(ruststream_send_family)]
pub mod in_process;
#[cfg(ruststream_send_family)]
pub mod lifecycle;
/// The refused-publish declaration of the local family's checks; the ladder of the `Send`
/// family is in the build with the crate's `alloc` feature, which the facade's `tokio` feature
/// turns on.
#[cfg(not(ruststream_send_family))]
pub mod lifecycle {
    pub use crate::unsent::Unsent;
}
#[cfg(ruststream_send_family)]
pub mod message_shape;
#[cfg(ruststream_send_family)]
pub mod retry;
#[cfg(ruststream_send_family)]
pub mod settlement;
mod unsent;
