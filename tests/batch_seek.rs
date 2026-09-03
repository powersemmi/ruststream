//! Repositioning a subscription from a page body: the broker gives the batch forms a
//! subscription-scoped context (the in-memory broker's [`MemoryBatchContext`] carries the
//! subscription's seeker), and where to seek rides the elements themselves - a
//! `&[Message<H, T>]` page reads the target off each element's typed header contract.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream::memory::{MemoryBatchContext, MemoryBroker, MemoryPosition, SeekHandle};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, Publisher, Seeker};

/// The producer's cursor contract: an element carrying `resume_at` asks the consumer to
/// reposition the subscription there once the page is settled.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Cursor {
    resume_at: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Entry {
    id: u64,
}

/// Settles the page, then repositions: the target comes from the elements' header contract and
/// the handle from the broker's subscription-scoped batch context.
#[subscriber("replay.log")]
async fn replay(
    page: &[Message<Cursor, Entry>],
    ctx: &mut Context<'_, MemoryBatchContext>,
) -> HandlerOutcome {
    let target = page
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
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Publishes `entry` with the cursor contract riding its headers.
///
/// This seeds the log BEFORE the subscription exists, which is the point of the test, so it goes
/// through the broker's own publisher rather than the harness's injection.
async fn publish_entry(broker: &MemoryBroker, id: u64, resume_at: Option<u64>) {
    let payload = serde_json::to_vec(&Entry { id }).expect("serializable");
    let msg = OutgoingMessage::new("replay.log", payload.as_slice())
        .with_typed_headers(&Cursor { resume_at })
        .expect("a flat header contract");
    broker.publisher().publish(msg).await.expect("publish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_reads_its_seek_target_from_element_headers() {
    let broker = MemoryBroker::new();

    // The whole run is in the log before the subscription opens, so the opening replay hands
    // the body one full page: the entries land at log positions 0, 1 and 2, and the first
    // element asks to resume from position 2 once the page is settled.
    publish_entry(&broker, 0, Some(2)).await;
    publish_entry(&broker, 1, None).await;
    publish_entry(&broker, 2, None).await;

    let app = RustStream::new(AppInfo::new("replay", "0.1.0")).with_broker(broker, |b| {
        b.include(replay.start_at(MemoryPosition::start()));
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    // Nothing is injected here: the reaction was started by the opening replay, so the harness
    // only has to drive it to a standstill.
    tb.settle().await.expect("the replay settles");

    let pages: Vec<Vec<u64>> = tb
        .broker::<MemoryBroker>()
        .subscriber("replay.log")
        .pages::<Entry>()
        .iter()
        .map(|page| page.iter().map(|entry| entry.id).collect())
        .collect();
    assert_eq!(
        pages,
        [vec![0, 1, 2], vec![2]],
        "the page after the seek must open at the header-named position",
    );
}
