//! What startup says about a registration's retry declaration: who applies the cap and the
//! dead-letter destination, the runtime that publishes the copies or the broker that moves them.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use ruststream::memory::prelude::*;
use std::future::ready;

use ruststream::memory::ConnectedMemoryBroker;
use ruststream::testing::TestApp;
use ruststream::{
    AddressedCopies, RedeliveryAddress, RedeliveryAddressed, Subscribe, Subscriber,
    SubscriptionSource, nonzero, subscriber,
};

use super::error::{Job, ManagedQueue, Queue, RETRY_DELAY};
use crate::log_capture;

/// Gives up after three tries, on a subscription whose copies this process publishes.
#[subscriber(Queue { name: "published" })]
async fn published_copies(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::retry()
}

/// Gives up after three tries, on a subscription whose broker moves the delivery itself.
#[subscriber(ManagedQueue { name: "moved" })]
async fn moved_by_the_broker(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::retry()
}

/// The cap and the destination are announced once at startup, with the subscription they belong
/// to and who applies them.
#[tokio::test(start_paused = true)]
async fn startup_names_who_applies_each_retry_declaration() {
    let captured = log_capture::Captured::default();
    let _installed = captured.install();
    let app =
        RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(published_copies)
                .max_attempts(nonzero!(3u32))
                .dead_letter("published.dead")
                .include(moved_by_the_broker)
                .max_attempts(nonzero!(5u32))
                .dead_letter("moved.dead")
        });
    let _tb = TestApp::start(app).await.expect("harness start");

    let lines = captured.lines_with("retry declaration;");
    let announced = |subscription: &str, applied_by: &str| {
        lines.iter().any(|line| {
            line.contains(&format!("subscription={subscription}"))
                && line.contains(&format!("applied_by=\"{applied_by}\""))
        })
    };
    assert!(announced("published", "runtime"), "{lines:?}");
    assert!(announced("moved", "broker"), "{lines:?}");
    for needle in [
        "max_attempts=Some(3)",
        "published.dead",
        "max_attempts=Some(5)",
        "moved.dead",
    ] {
        assert!(
            lines.iter().any(|line| line.contains(needle)),
            "{needle}: {lines:?}"
        );
    }
}

/// A subscription of a broker that cannot say where its retry copies go back to.
#[derive(Debug, Clone)]
struct Unaddressable {
    name: &'static str,
}

impl<C: Subscribe> SubscriptionSource<C> for Unaddressable
where
    C::Subscriber: Subscriber,
{
    type Subscriber = C::Subscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe(self.name).await
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker> for Unaddressable {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> {
        ready(Err(MemoryError::ShutDown))
    }
}

/// Asks for a retry later.
#[subscriber(Unaddressable { name: "unaddressable" })]
async fn unaddressed_copies(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A subscription that cannot name where its copies go stops the start, and says why.
#[tokio::test(start_paused = true)]
async fn a_subscription_that_cannot_address_its_copies_stops_the_start() {
    let app = RustStream::new(AppInfo::new("declared", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(unaddressed_copies));

    let Err(error) = TestApp::start(app).await else {
        panic!("the start is refused");
    };

    assert!(error.to_string().contains("shut down"), "{error}");
}
