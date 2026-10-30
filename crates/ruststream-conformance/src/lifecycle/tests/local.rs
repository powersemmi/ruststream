//! The local lifecycle against the in-memory bus with one fault switched on: a Send broker is a
//! broker of the local family through the blanket impls, so each fault reaches the local ladder
//! unchanged.

use crate::harness::local_lifecycle;

use super::{ConnectedFaulty, Fault, Faulty, FaultySource};

async fn local_ladder_on(fault: Fault) {
    local_lifecycle(
        move || Faulty::new(fault),
        |name| FaultySource(name.to_owned()),
        |connected: &ConnectedFaulty| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_broker_that_follows_the_ladder_passes_the_local_ladder() {
    local_ladder_on(Fault::None).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "local_lifecycle: a publish to")]
async fn a_publish_that_fails_fails_the_local_ladder() {
    local_ladder_on(Fault::PublishFails).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "the subscription must receive what was published to it")]
async fn a_mangled_payload_fails_the_local_ladder() {
    local_ladder_on(Fault::ManglesPayload).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "nack must succeed or be unsupported")]
async fn a_requeue_that_fails_fails_the_local_ladder() {
    local_ladder_on(Fault::RequeueFails).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "a publisher never used published successfully after the shutdown")]
async fn a_publisher_that_attaches_after_shutdown_fails_the_local_ladder() {
    local_ladder_on(Fault::UntouchedReconnects).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "local_lifecycle requeue after shutdown: stream timed out")]
async fn a_requeue_claimed_after_shutdown_fails_the_local_ladder() {
    local_ladder_on(Fault::ClaimsAfterShutdown).await;
}
