//! Conformance harness and generic in-memory [`MemoryBroker`] for broker authors.
//!
//! Any broker implementation can run itself through [`harness`] in one line to prove it
//! satisfies the contract defined by the core traits. Application tests that do not depend
//! on broker-specific semantics can use [`MemoryBroker`] directly.

pub mod harness;
pub mod helpers;
mod memory;

pub use memory::{MemoryBroker, MemoryMessage, MemoryPublisher, MemorySubscriber};
