//! Repositioning a live subscription with the `Seekable` capability: a `Seek` handler
//! parameter injects the subscription's own seeker, so the handler can skip forward past a
//! poison region without dropping the subscription.
//!
//! ```text
//! cargo run --example seek --features macros,memory,json
//! ```

use std::error::Error;
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySeeker, MemorySource};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, Seek};
use ruststream::{OutgoingMessage, Publisher, Seeker, subscriber};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:handler]
/// Skips forward when the producer marks a poison region: everything queued before the
/// resume point is dropped without touching the subscription itself.
#[subscriber(MemorySource::new("jobs"))]
async fn work(job: &Job, Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
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

    // --8<-- [start:mount]
    // Nothing is attached at the include site: the runtime mints the seeker off the
    // subscription itself right after it opens.
    let app = RustStream::new(AppInfo::new("seek-demo", "0.1.0")).with_broker(broker, |b| {
        b.include(work);
    });
    // --8<-- [end:mount]
    let running = app.start().await?;

    // The stream hits a poison marker at the second position; the handler's own seek jumps
    // it to the fourth, so id 3 is never processed.
    for id in [1, 999, 3, 4] {
        let payload = serde_json::to_vec(&Job { id })?;
        ingress
            .publish(OutgoingMessage::new("jobs", payload.as_slice()))
            .await?;
    }
    // A demo-only pause so the dispatch loop drains; a real service reacts to its own signals.
    sleep(Duration::from_millis(100)).await;

    running.shutdown().await?;
    println!("ok: skipped the poisoned region");
    Ok(())
}
