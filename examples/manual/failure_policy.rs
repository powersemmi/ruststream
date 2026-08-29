//! The unified failure policy without the `macros` feature: `on_failure(panic = .., decode = ..)`
//! is a settings step on the mount chain, so a definition built with `subscriber(..)` names the
//! same policies by chaining that step.
//!
//! ```text
//! cargo run --example manual_failure_policy --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:defaults]
/// No settings step: the defaults apply. A panic in the body fails fast (a loud error, then a
/// graceful shutdown so an orchestrator restarts the service); a payload that cannot decode is
/// dropped.
struct Process;

impl Handler<Order> for Process {
    // A body with nothing to await returns the future directly: `async fn` here would be an
    // unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("processing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn process_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(subscriber("orders", Process))
}
// --8<-- [end:defaults]

// --8<-- [start:tuned]
/// An untrusted topic: a handler bug should still take the service down (fail fast), but a
/// malformed message must not, so decode failures requeue instead of dropping or failing.
struct Ingest;

impl Handler<Order> for Ingest {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("ingesting order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn ingest_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(
        subscriber("ingest", Ingest).on_failure(
            FailurePolicies::default()
                .with_panic(FailurePolicy::FailFast)
                .with_decode(FailurePolicy::Retry),
        ),
    )
}
// --8<-- [end:tuned]

// --8<-- [start:skip]
/// A poison-tolerant consumer: move past anything that cannot be processed. A panic acks the
/// offending message and keeps consuming; a decode failure does the same.
struct Audit;

impl Handler<Order> for Audit {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("auditing order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

fn audit_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(
        subscriber("audit", Audit).on_failure(
            FailurePolicies::default()
                .with_panic(FailurePolicy::Skip)
                .with_decode(FailurePolicy::Skip),
        ),
    )
}
// --8<-- [end:skip]

fn app() -> RustStream {
    RustStream::new(AppInfo::new("failure-policy", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(process_routes());
        b.include_router(ingest_routes());
        b.include_router(audit_routes());
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
