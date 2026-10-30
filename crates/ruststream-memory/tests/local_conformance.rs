//! The conformance suites of the local family, run against `LocalMemoryBroker`, its reference
//! broker.

#![cfg(all(feature = "embassy", feature = "testing"))]

use ruststream_conformance::harness;
use ruststream_memory::local::{ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemorySource};

#[tokio::test(flavor = "current_thread")]
async fn the_local_bus_passes_the_routing_suite() {
    harness::run_local_suite(LocalMemoryBroker::<16>::new).await;
}

// `make_source` and `make_publisher` stay closures: their bounds are higher-ranked, and a bare
// method path binds one concrete lifetime.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "current_thread")]
async fn the_local_bus_passes_the_lifecycle() {
    harness::local_lifecycle(
        LocalMemoryBroker::<16>::new,
        |name| LocalMemorySource::new(name),
        |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
    )
    .await;
}
