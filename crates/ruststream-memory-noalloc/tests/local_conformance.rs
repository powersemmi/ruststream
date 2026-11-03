//! The local suites of the contract, run against the fixed bus: a bus of known size that the
//! board declares as a `static`, with the framework's `alloc` off.

#![cfg(feature = "testing")]

use ruststream_conformance::harness;
use ruststream_conformance::lifecycle::Unsent;
use ruststream_memory::local::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryBus, LocalMemorySource,
};

type Connected = ConnectedLocalMemoryBroker<16>;

/// The suites name their subjects at run time, and the bus holds a literal whole.
fn source(name: &str) -> LocalMemorySource {
    LocalMemorySource::new(Box::leak(name.to_owned().into_boxed_str()))
}

#[tokio::test(flavor = "current_thread")]
async fn the_fixed_bus_passes_the_routing_suite() {
    static BUS: LocalMemoryBus<16> = LocalMemoryBus::new();
    harness::run_local_suite(|| LocalMemoryBroker::new(&BUS)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_fixed_bus_passes_the_lifecycle() {
    static BUS: LocalMemoryBus<16> = LocalMemoryBus::new();
    harness::local_lifecycle(
        || LocalMemoryBroker::new(&BUS),
        source,
        |connected: &Connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_fixed_bus_returns_the_message_of_a_refused_publish() {
    static BUS: LocalMemoryBus<16> = LocalMemoryBus::new();
    harness::local_refused_publish_returns_message(
        || LocalMemoryBroker::new(&BUS),
        |connected: &Connected| connected.publisher(),
        Unsent::Returned,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_fixed_bus_keeps_the_readiness_contract() {
    static BUS: LocalMemoryBus<16> = LocalMemoryBus::new();
    harness::local_readiness(
        || LocalMemoryBroker::new(&BUS),
        source,
        |connected: &Connected| connected.publisher(),
    )
    .await;
}
