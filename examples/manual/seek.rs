//! Repositioning live subscriptions without the `macros` feature: `start_at(..)` is a settings
//! step chained on the mount, and the `Seek` parameter is a startup injection the runtime resolves
//! off the subscription - `with_seek` is its constructor, and the seeker reaches the body through
//! the same injection tuple the `Out` forms use.
//!
//! ```text
//! cargo run --example manual_seek --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::Seeker;
use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySeeker, MemorySource};
use ruststream::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:start_at]
/// The audit trail: its subscription opens at the start of the log, so entries published
/// before the service started are replayed into it. The position itself is a settings step, so
/// it is named where the handler is mounted, exactly as `workers` or `on_failure` would be.
struct Record;

impl Handler<Job> for Record {
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
    fn handle(&self, entry: &Job, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("audit: entry {}", entry.id);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:start_at]

// --8<-- [start:handler]
/// Skips forward when the producer marks a poison region: everything queued before the
/// resume point is dropped without touching the subscription itself.
struct Work;

/// The body, over any application state: what the attribute puts behind the `Seek(seeker)`
/// parameter. A body taking injected parameters is a `SlotsHandler` rather than a plain `Handler`:
/// the injection tuple names what the runtime prepares once the subscription opens, and
/// `Seek<MemorySeeker>` resolves off the subscriber itself, so the body holds a live seeker.
impl<S: Send + Sync> SlotsHandler<Job, (Seek<MemorySeeker>,), (), S> for Work {
    async fn handle(
        &self,
        job: &Job,
        (Seek(seeker),): &(Seek<MemorySeeker>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> Settle {
        if job.id == 999 {
            // The poison marker carries the resume point: skip to the fourth log entry.
            if seeker.seek(MemoryPosition::sequence(3)).await.is_err() {
                return HandlerResult::retry().into();
            }
            return HandlerResult::ack().into();
        }
        println!("jobs: processed {}", job.id);
        HandlerResult::ack().into()
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
        let payload = serde_json::to_vec(&Job { id })?;
        ingress.raw(&payload).to("audit").publish().await?;
    }

    // --8<-- [start:mount]
    // The runtime mints the seek handler's seeker off its subscription right after it opens, and
    // the chained start position seeks the audit one before its first delivery. `with_seek` names
    // the message and the seeker type; the source is the broker's own descriptor.
    let app = RustStream::new(AppInfo::new("seek-demo", "0.1.0")).with_broker(broker, |b| {
        b.include(with_seek::<Job, MemorySeeker, _, _>(
            MemorySource::new("jobs"),
            Work,
        ));
        b.include(subscriber(MemorySource::new("audit"), Record).start_at(MemoryPosition::start()));
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
