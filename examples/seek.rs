//! Repositioning live subscriptions through the broker's context keys: the in-memory broker
//! publishes a `SeekHandle` key over its per-delivery context, so a handler skips forward past
//! a poison region by reading it with the `Ctx` extractor, and a `start_at(..)` clause opens a
//! subscription at a chosen log position (replaying history into a fresh subscription, or
//! starting from the latest).
//!
//! ```text
//! cargo run --example seek --features macros,memory,json
//! ```

use std::error::Error;
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPosition, SeekHandle};
use ruststream::runtime::{AppInfo, Ctx, HandlerOutcome, PublishExt, RustStream};
use ruststream::{Seeker, subscriber};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:start_at]
/// The audit trail: its subscription opens at the start of the log, so entries published
/// before the service started are replayed into it.
#[subscriber("audit", start_at(MemoryPosition::start()))]
async fn record(entry: &Job) -> HandlerOutcome {
    println!("audit: entry {}", entry.id);
    HandlerOutcome::ack()
}
// --8<-- [end:start_at]

// --8<-- [start:handler]
/// Skips forward when the producer marks a poison region: everything queued before the
/// resume point is dropped without touching the subscription itself. The seeker is a broker
/// context field - the `SeekHandle` key reads it off the delivery's context.
#[subscriber("jobs")]
async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if job.id == 999 {
        // The poison marker carries the resume point: skip to the fourth log entry.
        if seeker.seek(MemoryPosition::sequence(3)).await.is_err() {
            return HandlerOutcome::retry();
        }
        return HandlerOutcome::ack();
    }
    println!("jobs: processed {}", job.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    // Published before the app even exists; only the chosen start position below makes these
    // visible to the audit subscription.
    for id in 1..=2u64 {
        let payload = serde_json::to_vec(&Job { id })?;
        ingress.raw(&payload).to("audit").publish().await?;
    }

    // --8<-- [start:mount]
    // Both mount plainly: the seek handler's context is built off its own subscription per
    // delivery, and the start_at clause seeks the audit one before its first delivery.
    let app = RustStream::new(AppInfo::new("seek-demo", "0.1.0")).with_broker(broker, |b| {
        b.include(work);
        b.include(record);
    });
    // --8<-- [end:mount]
    let running = app.start().await?;

    // The jobs stream hits a poison marker at the second position; the handler's own seek
    // jumps it to the fourth, so id 3 is never processed.
    for id in [1, 999, 3, 4] {
        let payload = serde_json::to_vec(&Job { id })?;
        ingress.raw(&payload).to("jobs").publish().await?;
    }
    // A demo-only pause so the dispatch loops drain; a real service reacts to its own signals.
    sleep(Duration::from_millis(100)).await;

    running.shutdown().await?;
    println!("ok: replayed the audit history and skipped the poisoned region");
    Ok(())
}
