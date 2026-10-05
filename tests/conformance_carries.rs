//! The carried lane's conformance checks against a subscription that carries rows: the reference
//! passes them, and each check fails against a broker broken in exactly the way it exists to
//! catch.

#![cfg(all(feature = "conformance", feature = "memory"))]

mod carrying;

use std::time::Duration;

use carrying::{Fault, Row, Rows};
use ruststream::conformance::capabilities;
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError};
use ruststream::{OutgoingMessage, Publisher};
use tokio::time::timeout;

/// How long a check that must fail at once may run before the test calls it a hang.
const HANG_LIMIT: Duration = Duration::from_secs(5);

/// The rows a check publishes: distinct, so a value lent twice or lent out of place shows.
fn rows() -> [Row; 3] {
    [Row::new(1, "ann"), Row::new(2, "bob"), Row::new(3, "cyd")]
}

/// Publishes one row the way the broker's own producer writes it.
async fn publish_row(
    connected: &ConnectedMemoryBroker,
    subject: &str,
    row: &Row,
) -> Result<(), MemoryError> {
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(subject, row.payload().as_bytes()),
            None,
        )
        .await
}

/// Publishes one delivery whose row is gone by the time it is read.
async fn publish_gone(connected: &ConnectedMemoryBroker, subject: &str) -> Result<(), MemoryError> {
    connected
        .publisher()
        .publish(OutgoingMessage::new(subject, b"gone".as_slice()), None)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carrying_subscription_passes_carries() {
    capabilities::carries(
        MemoryBroker::new,
        |name| Rows::new(name),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delivery lent a value that was not published")]
async fn a_delivery_lending_a_stale_value_fails_carries() {
    capabilities::carries(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::StaleValue),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delivery without a value must lend none")]
async fn a_delivery_lending_a_placeholder_fails_carries() {
    capabilities::carries(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::PlaceholderForGone),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carrying_subscription_passes_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the value at index `i` must belong to the `i`-th delivery")]
async fn a_batch_lending_its_values_out_of_order_fails_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::ReversedPage),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a batch must never lend more values than it delivers")]
async fn a_batch_lending_more_values_than_deliveries_fails_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::ExtraRow),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delivery without a value must come after every delivery with one")]
async fn a_batch_keeping_a_delivery_without_a_value_in_front_fails_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::GoneFirst),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delivery without a value must be lent none")]
async fn a_batch_lending_a_placeholder_fails_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::PlaceholderForGone),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_that_never_groups_its_deliveries_passes_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::OnePerPage),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_cannot_requeue_passes_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::NoRequeue),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_with_one_delivery_in_flight_passes_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::OneInFlight),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a copy of a delivery, published from its payload and headers")]
async fn a_delivery_whose_copy_loses_its_value_fails_carries() {
    capabilities::carries(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::EmptyPayload),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a copy of a delivery, published from its payload and headers")]
async fn a_batch_whose_copies_lose_their_values_fails_carries_batch() {
    capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::EmptyPayload),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a yielded batch must not be empty")]
async fn a_subscription_yielding_empty_batches_fails_carries_batch() {
    let rows = rows();
    let suite = capabilities::carries_batch(
        MemoryBroker::new,
        |name| Rows::new(name).faulty(Fault::EmptyPages),
        ConnectedMemoryBroker::publisher,
        publish_row,
        publish_gone,
        &rows,
    );
    timeout(HANG_LIMIT, suite)
        .await
        .expect("carries_batch must fail against empty batches, not hang");
}
