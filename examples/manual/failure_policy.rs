//! The unified failure policy without the `macros` feature: `on_failure(panic = .., decode = ..)`
//! is a settings step on the mount chain, so a definition built with `subscriber(..)` names the
//! same policies by chaining that step.
//!
//! ```text
//! cargo run --example manual_failure_policy --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// --8<-- [start:defaults]
/// No settings step: the defaults apply. A panic in the body fails fast (a loud error, then a
/// graceful shutdown so an orchestrator restarts the service); a payload that cannot decode is
/// dropped.
struct Process;

impl Handle<Order> for Process {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("processing order {}", order.id);
        ready(Ok(()))
    }
}

fn process_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(subscriber("orders", Process).build())
}
// --8<-- [end:defaults]

// --8<-- [start:tuned]
/// An untrusted topic: a handler bug should still take the service down (fail fast), but a
/// malformed message must not, so decode failures requeue instead of dropping or failing.
struct Ingest;

impl Handle<Order> for Ingest {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("ingesting order {}", order.id);
        ready(Ok(()))
    }
}

fn ingest_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(
        subscriber("ingest", Ingest)
            .on_failure(
                FailurePolicies::default()
                    .with_panic(FailurePolicy::FailFast)
                    .with_decode(FailurePolicy::Retry),
            )
            .build(),
    )
}
// --8<-- [end:tuned]

// --8<-- [start:skip]
/// A poison-tolerant consumer: move past anything that cannot be processed. A panic acks the
/// offending message and keeps consuming; a decode failure does the same.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("auditing order {}", order.id);
        ready(Ok(()))
    }
}

fn audit_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new().include(
        subscriber("audit", Audit)
            .on_failure(
                FailurePolicies::default()
                    .with_panic(FailurePolicy::Skip)
                    .with_decode(FailurePolicy::Skip),
            )
            .build(),
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
