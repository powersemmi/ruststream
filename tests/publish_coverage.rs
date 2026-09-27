//! The publishing handler's failure paths, end to end over the memory broker: a decode failure
//! settled by the per-subscriber policy, and a reply the publisher rejects. What the warnings of
//! both paths carry is pinned beside the handler, in its unit tests.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::runtime::RustStreamError;
use ruststream::testing::{Outcome, TestApp};
use ruststream::{BytesMut, OutgoingMessage, PairError, Take};
use serde::Serialize;

use common::{Order, Wire};

/// The reply publisher's refusal.
#[derive(Debug)]
struct Rejected;

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the reply publisher rejected the message")
    }
}

impl StdError for Rejected {}

/// Rejects the first reply, then forwards to the broker: the redelivery the failure asks for
/// must be able to succeed, so the whole failure-then-retry path is observable.
struct FailsOnce {
    armed: AtomicBool,
    inner: MemoryPublisher,
}

impl Publisher for FailsOnce {
    type Payload = Take;
    type Error = Rejected;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(Rejected);
        }
        self.inner.publish(msg, options).await.map_err(|_| Rejected)
    }
}

/// The declaration half: pure policy, paired at startup like any broker's.
struct FailsOncePolicy;

impl PublishPolicy<ConnectedMemoryBroker> for FailsOncePolicy {
    type Live = FailsOnce;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(FailsOnce {
            armed: AtomicBool::new(true),
            inner: Publish.pair(connected).await?,
        })
    }
}

/// What the publishing handlers below answer with: the id alone, under a declared name, because
/// a reply is published like any other message.
#[derive(Debug, Serialize, Outgoing)]
struct Acked(u32);

/// A publishing handler whose decode failure is declared fatal.
#[subscriber("pubff", publish("pubff.out"), on_failure(decode = fail_fast))]
async fn pubff(order: &Order) -> Acked {
    Acked(order.id)
}

/// A publishing handler whose reply leaves through the publisher that fails once.
#[subscriber("flaky", publish("flaky.out"))]
async fn flaky(order: &Order) -> Acked {
    Acked(order.id)
}

/// `decode = fail_fast` on a publishing handler tears the service down, and the run reports it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fail_fast_decode_failure_tears_the_service_down() {
    let app =
        RustStream::new(AppInfo::new("pubff", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(pubff);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Wire::of(b"not json"))
        .to("pubff")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber("pubff")
        .assert_called_once()
        .assert_last_failed_to_decode();
    tb.assert_shut_down();
    let result = tb.shutdown().await;

    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "the run must report the failure, got {result:?}",
    );
}

/// A reply the publisher rejects nacks the delivery with requeue instead of losing the reply:
/// the redelivered message publishes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_reply_publish_retries_the_delivery() {
    let app =
        RustStream::new(AppInfo::new("flaky", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(flaky).out(Reply, FailsOncePolicy);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 7 })
        .to("flaky")
        .publish()
        .await
        .expect("publish failed");

    // The first attempt nacks with requeue rather than losing the reply; the redelivery publishes
    // it exactly once.
    assert_eq!(
        tb.broker::<MemoryBroker>().subscriber("flaky").outcomes(),
        [Outcome::Nack, Outcome::Ack],
    );
    tb.broker::<MemoryBroker>()
        .published::<u32>("flaky.out")
        .assert_called_once()
        .with_raw(b"7");
}
