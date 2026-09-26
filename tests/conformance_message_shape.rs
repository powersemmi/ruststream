//! Runs the opt-in message-shape checks against the reference `MemoryBroker`.
//!
//! The part every broker owes runs inside `harness::lifecycle` (see `conformance_self.rs`);
//! these are the checks that take an input only the broker can supply.
#![cfg(all(feature = "conformance", feature = "memory"))]

use ruststream::Bytes;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::message_shape::{self, OptionCases};
#[cfg(feature = "asyncapi")]
use ruststream::memory::ConnectedMemoryBroker;
use ruststream::memory::{MemoryBroker, MemoryPublish, MemorySource, PARTITION_KEY_HEADER};

// --8<-- [start:keyed_order]
// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked, so a bare
// method path, which binds one concrete lifetime, would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_keeps_keys_and_their_order() {
    message_shape::keyed_order(
        MemoryBroker::new,
        &unique_subject("conformance.keyed"),
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
        // Where a key goes on this broker: a header here, a field of the options elsewhere.
        |key, headers| {
            headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
            None
        },
    )
    .await;
}
// --8<-- [end:keyed_order]

// --8<-- [start:publish_options]
/// The in-memory broker has no per-message settings, so its cases are a single unit override. A
/// broker with settings configures the policy away from the transport's default and names a value
/// the transport refuses.
#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_resolves_its_options() {
    message_shape::publish_options(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        MemoryPublish,
        OptionCases::new(()).overrides((), ()),
        |_delivery| (),
    )
    .await;
}
// --8<-- [end:publish_options]

// --8<-- [start:credentials]
#[cfg(feature = "asyncapi")]
#[test]
fn memory_publish_policy_binds_no_credentials() {
    message_shape::publishes_without_credentials::<ConnectedMemoryBroker, _>(
        &MemoryPublish,
        "hunter2",
    );
}
// --8<-- [end:credentials]
