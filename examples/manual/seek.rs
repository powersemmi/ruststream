//! Repositioning live subscriptions without the `macros` feature: `start_at(..)` is a settings
//! step chained on the mount, and the seeker rides the broker context axis - a body that
//! declares the in-memory broker's `MemoryContext` reads its position and its reposition handle
//! by key, straight through `Context`.
//!
//! ```text
//! cargo run --example manual_seek --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
struct Job {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the destination form is the one that leaves the
// name to the call, so the publish builder offers `to(..)`.
impl OutgoingDestination for Job {
    type Form = CallerName;
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

// --8<-- [start:start_at]
/// The audit trail: its subscription opens at the start of the log, so entries published
/// before the service started are replayed into it. The position itself is a settings step, so
/// it is named where the handler is mounted, exactly as `workers` or `on_failure` would be.
struct Record;

impl Handle<Job> for Record {
    fn handle(
        &self,
        entry: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("audit: entry {}", entry.id);
        ready(Ok(()))
    }
}
// --8<-- [end:start_at]

// --8<-- [start:handler]
/// Skips forward when the producer marks a poison region: everything queued before the
/// resume point is dropped without touching the subscription itself.
struct Work;

/// The body behind the attribute's `Ctx(seeker)` parameter: seeking is the broker context axis
/// of `Handle`, so declaring the broker's own `MemoryContext` is the whole declaration - the
/// runtime builds one per delivery off the subscription, and the `SeekHandle` key reads the
/// reposition handle out of it.
impl Handle<Job, (), (), MemoryContext> for Work {
    async fn handle(
        &self,
        job: &Job,
        _outs: &(),
        ctx: &mut Context<'_, MemoryContext>,
    ) -> Result<(), HandlerOutcome> {
        if job.id == 999 {
            // The poison marker carries the resume point: skip to the fourth log entry.
            if ctx
                .context(SeekHandle)
                .seek(MemoryPosition::sequence(3))
                .await
                .is_err()
            {
                return Err(HandlerOutcome::retry());
            }
            return Ok(());
        }
        println!("jobs: processed {}", job.id);
        Ok(())
    }
}
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    // Published before the app even exists; only the chosen start position below makes these
    // visible to the audit subscription.
    for id in 1..=2u64 {
        ingress.message(&Job { id }).to("audit").publish().await?;
    }

    // --8<-- [start:mount]
    // Both mount plainly: the seek body's context is built off its own subscription per
    // delivery, and the chained start position seeks the audit one before its first delivery.
    let app = RustStream::new(AppInfo::new("seek-demo", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(MemorySource::new("jobs"), Work).build());
        // --8<-- [start:start_at_mount]
        b.include(
            subscriber(MemorySource::new("audit"), Record)
                .start_at(MemoryPosition::start())
                .build(),
        );
        // --8<-- [end:start_at_mount]
    });
    // --8<-- [end:mount]
    let running = app.start().await?;

    // The jobs stream hits a poison marker at the second position; the handler's own seek
    // jumps it to the fourth, so id 3 is never processed.
    for id in [1, 999, 3, 4] {
        ingress.message(&Job { id }).to("jobs").publish().await?;
    }
    // A demo-only pause so the dispatch loops drain; a real service reacts to its own signals.
    sleep(Duration::from_millis(100)).await;

    running.shutdown().await?;
    println!("ok: replayed the audit history and skipped the poisoned region");
    Ok(())
}
