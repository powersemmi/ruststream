//! The local routing suite against the in-process transports with one fault switched on: a Send
//! broker is a broker of the local family through the blanket impls.

use crate::harness::run_local_suite;

use super::{Fault, faulty};

#[tokio::test(flavor = "current_thread")]
async fn an_honest_transport_passes_the_local_routing_suite() {
    run_local_suite(faulty(Fault::Honest)).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "the publish log must record what the broker's own publisher sent")]
async fn a_log_without_the_brokers_own_publishes_fails_the_local_suite() {
    run_local_suite(faulty(Fault::LogsInjectedOnly)).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "must be counted in flight before `inject` returns")]
async fn a_transport_that_counts_nothing_fails_the_local_suite() {
    run_local_suite(faulty(Fault::Uncounted)).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "a publish no subscription receives must not be counted")]
async fn a_transport_that_counts_an_unread_publish_fails_the_local_suite() {
    run_local_suite(faulty(Fault::CountsUnread)).await;
}
