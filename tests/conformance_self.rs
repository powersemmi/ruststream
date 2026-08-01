//! Runs the conformance harness against the reference `MemoryBroker` impl.
//!
//! If this test fails, either `MemoryBroker` regressed or the harness expectations are
//! inconsistent.

use ruststream::{
    conformance::{capabilities, harness},
    memory::{MemoryBroker, MemorySource},
};

// --8<-- [start:run_suite]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_conformance_suite() {
    harness::run_suite(MemoryBroker::new).await;
}
// --8<-- [end:run_suite]

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

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_request_reply_suite() {
    capabilities::request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| broker.requester(),
        |broker| broker.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_batches_suite() {
    capabilities::batches(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_transactions_suite() {
    capabilities::transactions(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_passes_seeking_suite() {
    capabilities::seeking(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| broker.publisher(),
    )
    .await;
}
