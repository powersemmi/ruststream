//! A batch that answers and reads the broker's subscription-scoped context in one body.
//!
//! The reply axis and the context axis are independent: naming the broker's batch context
//! (`MemoryBatchContext` here) must leave the `publish(..)` clause intact, and the reposition
//! handle the context carries must be live while the replies are produced.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream::Seeker;
use ruststream::memory::{MemoryBatchContext, MemoryBroker, MemoryPosition, SeekHandle};
use ruststream::prelude::*;
use ruststream::testing::TestApp;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct Digest {
    id: u64,
}

/// The producer's cursor contract: an element carrying `resume_at` asks the consumer to
/// reposition the subscription to that log position once the batch is settled.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Cursor {
    resume_at: Option<u64>,
}

/// One digest per order, produced while the body holds the broker's batch context.
#[subscriber("orders", publish("digests"))]
async fn digest(batch: &[Order], ctx: &mut Context<'_, MemoryBatchContext>) -> Vec<Digest> {
    // Reading the key is what proves the batch context reached a replying body; the handle it
    // yields is the subscription's own.
    let _seeker = ctx.context(SeekHandle);
    batch.iter().map(|order| Digest { id: order.id }).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replying_batch_reads_the_brokers_batch_context() {
    let app =
        RustStream::new(AppInfo::new("digests", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(digest.batch(nonzero!(8)));
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 7 })
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Digest>("digests")
        .assert_called_once()
        .with(&Digest { id: 7 });
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}

/// The same body, repositioning the subscription through the batch context before it answers:
/// where to resume rides the elements' own header contract, since a batch context carries no
/// per-delivery data.
#[subscriber("replay.orders", publish("replay.digests"))]
async fn replay_digest(
    batch: &[Message<Cursor, Order>],
    ctx: &mut Context<'_, MemoryBatchContext>,
) -> Result<Vec<Digest>, HandlerOutcome> {
    let target = batch
        .iter()
        .find_map(|element| element.headers.resume_at)
        .map(|sequence| usize::try_from(sequence).expect("test positions are small"));
    if let Some(sequence) = target
        && ctx
            .context(SeekHandle)
            .seek(MemoryPosition::sequence(sequence))
            .await
            .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(batch
        .iter()
        .map(|element| Digest {
            id: element.body.id,
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replying_batch_repositions_through_the_batch_context() {
    let app =
        RustStream::new(AppInfo::new("replay", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(replay_digest.batch(nonzero!(8)));
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // The first order lands at log position 0 and asks the consumer to resume at position 1,
    // the slot the next publish takes: the reposition is real, and it neither loses nor
    // duplicates anything.
    tb.broker::<MemoryBroker>()
        .publish_with_headers(
            "replay.orders",
            &Order { id: 1 },
            &Cursor { resume_at: Some(1) },
        )
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .publish_with_headers(
            "replay.orders",
            &Order { id: 2 },
            &Cursor { resume_at: None },
        )
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    // A failed reposition would settle the batch as a retry, so the acks are what say the handle
    // the batch context carries was live.
    tb.broker::<MemoryBroker>()
        .subscriber("replay.orders")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    let published = tb
        .broker::<MemoryBroker>()
        .published::<Digest>("replay.digests");
    let ids: Vec<u64> = published.decoded().iter().map(|reply| reply.id).collect();
    assert_eq!(
        ids,
        [1, 2],
        "the subscription must resume at the named position, answering each order once",
    );

    tb.shutdown().await.expect("graceful shutdown");
}
