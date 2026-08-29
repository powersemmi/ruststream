//! Repositioning live subscriptions without the `macros` feature: the definitions the attribute
//! would generate, written out. The `Seek` parameter is a startup injection the runtime resolves
//! off the subscription, and `start_at(..)` is a builder step on the declaration, so both are
//! trait impls on a named type here.
//!
//! ```text
//! cargo run --example manual_seek --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySeeker, MemorySource};
use ruststream::prelude::*;
use ruststream::runtime::{
    Declared, Decoded, Fixed, Handler, IncludeDef, InjectCall, InjectDef, Open, Settle,
    SubscriberBuilder, SubscriberDef, SubscriberSettings, forms,
};
use ruststream::{Seeker, StartAt};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:start_at]
/// The audit trail: its subscription opens at the start of the log, so entries published
/// before the service started are replayed into it.
struct Record;

impl Handler<Job> for Record {
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
    fn handle(&self, entry: &Job, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("audit: entry {}", entry.id);
        ready(HandlerResult::ack().into())
    }
}

impl SubscriberDef for Record {
    type Input = Decoded<Job>;
    type Context = ();
    type Handler = Self;
    type Source = MemorySource;

    fn source(&self) -> MemorySource {
        MemorySource::new("audit")
    }

    fn into_handler(self) -> Self {
        self
    }
}

/// The start position is a settings step over the definition, and `start_at(..)` in the
/// attribute is this chain: it decorates the source with [`StartAt`], which is what the state
/// tuple records as `Fixed`. A definition declaring no settings implements
/// [`IncludeDef`](ruststream::runtime::IncludeDef) instead and gets `Declared` for free, the way
/// `Work` below does.
impl Declared for Record {
    type Form = forms::Subscribing;
    type Settings =
        SubscriberBuilder<Self, StartAt<MemorySource, MemoryPosition>, (Open, Open, Fixed)>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, MemorySource::new("audit")).start_at(MemoryPosition::start())
    }
}
// --8<-- [end:start_at]

// --8<-- [start:handler]
/// Skips forward when the producer marks a poison region: everything queued before the
/// resume point is dropped without touching the subscription itself.
struct Work;

impl IncludeDef for Work {
    type Form = forms::Seek;
}

/// A handler taking injected parameters is an `InjectDef` rather than a plain `SubscriberDef`:
/// the injection tuple names what the runtime prepares once the subscription opens, and
/// `Seek<MemorySeeker>` resolves off the subscriber itself, so the body holds a live seeker.
impl InjectDef for Work {
    type Input = Decoded<Job>;
    type Context = ();
    type Source = MemorySource;
    type Injections = (Seek<MemorySeeker>,);

    fn source(&self) -> MemorySource {
        MemorySource::new("jobs")
    }
}

/// The body, over any application state: what the attribute puts in `InjectCall::call`, with the
/// injections destructured out of the tuple exactly as the parameter patterns would.
impl<S: Send + Sync> InjectCall<S> for Work {
    async fn call(
        &self,
        job: &Job,
        (Seek(seeker),): &Self::Injections,
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
    // Both mount plainly: the runtime mints the seek handler's seeker off its subscription
    // right after it opens, and the declaration's start position seeks the audit one before its
    // first delivery.
    let app = RustStream::new(AppInfo::new("seek-demo", "0.1.0")).with_broker(broker, |b| {
        b.include(Work);
        b.include(Record);
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
