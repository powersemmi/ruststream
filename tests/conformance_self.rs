//! Runs the conformance harness against the reference `MemoryBroker` impl.
//!
//! If this test fails, either `MemoryBroker` regressed or the harness expectations are
//! inconsistent.

use std::convert::Infallible;

use ruststream::{conformance::harness, memory::MemoryBroker};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_conformance_suite() {
    harness::run_suite(|| async { Ok::<_, Infallible>(MemoryBroker::new()) }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_batch_publisher_suite() {
    harness::run_batch_publisher_suite(|| async { Ok::<_, Infallible>(MemoryBroker::new()) }).await;
}
