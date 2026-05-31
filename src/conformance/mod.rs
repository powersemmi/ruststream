//! Conformance harness for broker authors.
//!
//! Any broker implementation can run itself through [`harness`] in one line to prove it
//! satisfies the contract defined by the core traits. The harness runs against the
//! reference [`crate::memory::MemoryBroker`], which lives in the always-available `memory`
//! module so applications can use it without the conformance feature.

pub mod harness;
pub mod helpers;
