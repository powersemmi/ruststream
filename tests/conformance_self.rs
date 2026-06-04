//! Runs the conformance harness against the reference `MemoryBroker` impl.
//!
//! If this test fails, either `MemoryBroker` regressed or the harness expectations are
//! inconsistent.

use std::convert::Infallible;

use ruststream::{
    conformance::harness,
    memory::{MemoryBroker, MemorySource},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_conformance_suite() {
    harness::run_suite(|| async { Ok::<_, Infallible>(MemoryBroker::new()) }).await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_lifecycle() {
    harness::lifecycle(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
    )
    .await;
}
