//! The `TestApp` harness of [RustStream](https://docs.rs/ruststream): it runs a service's
//! production app in process, or against running brokers, and reads what its handlers did.
//!
//! Services use it through the `ruststream` facade, as
//! [`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/), whose overview
//! teaches it.

// The harness polls the app on the test's own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

#[cfg(feature = "embassy")]
pub mod embedded;
#[cfg(feature = "tokio")]
mod handle;
#[cfg(feature = "tokio")]
mod harness;

#[cfg(feature = "tokio")]
pub use handle::{BrokerHandle, InjectSink};
#[cfg(feature = "tokio")]
pub use harness::{TestApp, TestBrokers};
