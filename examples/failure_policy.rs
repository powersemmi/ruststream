//! The unified failure policy from the failure-policy guide: `on_failure(panic = .., decode = ..)`
//! sets, per subscriber, what happens when a handler panics or a payload cannot decode.
//!
//! ```text
//! cargo run --example failure_policy --features macros,memory,json -- run
//! ```

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:defaults]
/// No clause: the defaults apply. A panic in the body fails fast (a loud error, then a graceful
/// shutdown so an orchestrator restarts the service); a payload that cannot decode is dropped.
#[subscriber("orders")]
async fn process(order: &Order) -> HandlerOutcome {
    println!("processing order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:defaults]

// --8<-- [start:tuned]
/// An untrusted topic: a handler bug should still take the service down (fail fast), but a
/// malformed message must not, so decode failures requeue instead of dropping or failing.
#[subscriber("ingest", on_failure(panic = fail_fast, decode = retry))]
async fn ingest(order: &Order) -> HandlerOutcome {
    println!("ingesting order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:tuned]

// --8<-- [start:skip]
/// A poison-tolerant consumer: move past anything that cannot be processed. A panic acks the
/// offending message and keeps consuming; a decode failure does the same.
#[subscriber("audit", on_failure(panic = skip, decode = skip))]
async fn audit(order: &Order) -> HandlerOutcome {
    println!("auditing order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:skip]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("failure-policy", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(process);
        b.include(ingest);
        b.include(audit);
    })
}
