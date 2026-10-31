//! The conformance suite of [RustStream](https://docs.rs/ruststream): the contract checks a broker
//! crate runs against its in-process transport and against a running broker.
//!
//! Broker crates use it through the `ruststream` facade, as
//! [`ruststream::conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/), whose
//! overview teaches it.

// The conformance suites drive a broker's cold path, whose futures carry no Send.
#![allow(clippy::future_not_send)]

pub mod capabilities;
pub mod harness;
pub mod helpers;
pub mod in_process;
pub mod lifecycle;
pub mod message_shape;
pub mod retry;
pub mod settlement;
