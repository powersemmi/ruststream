//! The snippet source for the broker-authors Conformance guide: the lifecycle and capability
//! checks against a real broker, exactly as a broker crate would write them. Gated behind
//! `NATS_TEST_URL` because both perform a real `connect`.
#![cfg(feature = "conformance")]
// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check (clippy's suggestion is a false positive here, as in conformance_self.rs).
// Allowed file-wide so the embedded doc snippets stay free of clippy attributes.
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use ruststream::conformance::{capabilities, harness};
use ruststream_nats::{NatsBroker, SubscribeOptions};

// --8<-- [start:lifecycle]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_lifecycle() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    harness::lifecycle(
        || NatsBroker::new(url.clone()), // sync construction (no I/O)
        |subject| SubscribeOptions::new(subject), // the broker's SubscriptionSource
        |broker| broker.publisher(),     // a publisher from the connected broker
    )
    .await;
}
// --8<-- [end:lifecycle]

// --8<-- [start:request_reply]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_request_reply() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    capabilities::request_reply(
        || NatsBroker::new(url.clone()),
        |subject| SubscribeOptions::new(subject),
        |broker| broker.publisher(), // the RequestReply publisher under test
        |broker| broker.publisher(), // the plain publisher the responder replies through
    )
    .await;
}
// --8<-- [end:request_reply]
