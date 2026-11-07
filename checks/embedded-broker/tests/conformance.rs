//! The local suites of the contract, run against a broker whose types are never `Send`, through
//! the facade's `conformance` feature beside the embedded runner.

use ruststream::conformance::harness;
use ruststream::conformance::lifecycle::Unsent;
use ruststream_embedded_broker::{ConnectedRcBroker, RcBroker, RcPublisher, RcSource};

/// The suites name their subjects at run time, and a subscription of the bus holds its name for
/// as long as the bus runs.
fn source(name: &str) -> RcSource {
    RcSource::new(Box::leak(name.to_owned().into_boxed_str()))
}

fn publisher(connected: &ConnectedRcBroker) -> RcPublisher {
    connected.publisher()
}

#[tokio::test(flavor = "current_thread")]
async fn passes_the_routing_suite() {
    harness::run_local_suite(RcBroker::new).await;
}

#[tokio::test(flavor = "current_thread")]
async fn keeps_the_lifecycle() {
    harness::local_lifecycle(RcBroker::new, source, publisher).await;
}

#[tokio::test(flavor = "current_thread")]
async fn returns_the_message_of_a_refused_publish() {
    harness::local_refused_publish_returns_message(RcBroker::new, publisher, Unsent::Returned)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn keeps_the_readiness_contract() {
    harness::local_readiness(RcBroker::new, source, publisher).await;
}
