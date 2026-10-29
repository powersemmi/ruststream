//! The `TestApp` harness of [RustStream](https://docs.rs/ruststream): it runs a service's
//! production app in process, or against running brokers, and reads what its handlers did.
//!
//! Services use it through the `ruststream` facade, as
//! [`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/), whose overview
//! teaches it.

mod handle;
mod harness;

pub use handle::{BrokerHandle, InjectSink};
pub use harness::{TestApp, TestBrokers};
