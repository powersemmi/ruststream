//! Repositioning live subscriptions with the `Seekable` capability: a `WithSeeker` token
//! repositions a runtime-owned subscription from outside (replay after a fix, reprocessing),
//! and a `Seek` handler parameter repositions its own subscription from inside (skipping
//! forward past a poison region).
//!
//! ```text
//! cargo run --example seek --features macros,memory,json
//! ```

use std::error::Error;
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySeeker, MemorySource};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, Seek};
use ruststream::{OutgoingMessage, Publisher, Seeker, WithSeeker, subscriber};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    id: u64,
}

/// The audit trail: a plain subscriber whose subscription is repositioned from outside.
#[subscriber(MemorySource::new("audit"))]
async fn record(entry: &Entry) -> HandlerResult {
    println!("audit: entry {}", entry.id);
    HandlerResult::Ack
}

// --8<-- [start:handler]
/// Skips forward when the producer marks a poison region: everything queued before the
/// resume point is dropped without touching the subscription itself.
#[subscriber(MemorySource::new("jobs"))]
async fn work(job: &Entry, Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
    if job.id == 999 {
        // The poison marker carries the resume point: skip to the fourth log entry.
        if seeker.seek(MemoryPosition::sequence(3)).await.is_err() {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    println!("jobs: processed {}", job.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    // --8<-- [start:attach]
    // Wrap the source before mounting; the token resolves to the live seeker at startup.
    let (audit_source, audit_token) = WithSeeker::attach(MemorySource::new("audit"));
    let app = RustStream::new(AppInfo::new("seek-demo", "0.1.0")).with_broker(broker, |b| {
        b.include_on(audit_source, record);
        b.include(work);
    });
    let running = app.start().await?;
    // --8<-- [end:attach]

    // The audit trail sees three entries; the jobs stream hits a poison marker at the second
    // position and the handler's own seek jumps it to the fourth.
    for id in 1..=3u64 {
        let payload = serde_json::to_vec(&Entry { id })?;
        ingress
            .publish(OutgoingMessage::new("audit", payload.as_slice()))
            .await?;
    }
    for id in [1, 999, 3, 4] {
        let payload = serde_json::to_vec(&Entry { id })?;
        ingress
            .publish(OutgoingMessage::new("jobs", payload.as_slice()))
            .await?;
    }
    // A demo-only pause so the dispatch loops drain; a real service reacts to its own signals.
    sleep(Duration::from_millis(100)).await;

    // --8<-- [start:redeem]
    // Ops-style replay from outside the handlers: rewind the audit subscription to the start
    // of its log. The token resolves only after startup; before that it reports pending.
    let seeker = audit_token.seeker()?;
    seeker.seek(MemoryPosition::start()).await?;
    // --8<-- [end:redeem]
    sleep(Duration::from_millis(100)).await;

    running.shutdown().await?;
    println!("ok: replayed the audit log and skipped the poisoned job region");
    Ok(())
}
