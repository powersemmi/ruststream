//! The unified failure policy without the `macros` feature: `on_failure(panic = .., decode = ..)`
//! is a settings step on the definition's own builder, so a hand-written definition names the same
//! policies by writing that step out.
//!
//! A definition is two impls: `SubscriberDef` (what to run, on which source) and `Declared` (which
//! settings the declaration fixes). `include` reads both, exactly as it does for a generated one.
//!
//! ```text
//! cargo run --example manual_failure_policy --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, Declared, Decoded, FailurePolicies, FailurePolicy, Fixed, Handler, Open, Settle,
    SubscriberBuilder, SubscriberDef, forms,
};
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

impl SubscriberDef for Process {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("orders")
    }

    fn into_handler(self) -> Self {
        self
    }
}

impl Declared for Process {
    type Form = forms::Subscribing;
    // Every setting still open: nothing is fixed here, so the mount site could still name them.
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("orders"))
    }
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

impl SubscriberDef for Ingest {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("ingest")
    }

    fn into_handler(self) -> Self {
        self
    }
}

impl Declared for Ingest {
    type Form = forms::Subscribing;
    // The middle slot is `Fixed`: the policies are named here, so naming them again at the mount
    // site does not compile.
    type Settings = SubscriberBuilder<Self, Name, (Open, Fixed, Open)>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("ingest")).on_failure(
            FailurePolicies::default()
                .with_panic(FailurePolicy::FailFast)
                .with_decode(FailurePolicy::Retry),
        )
    }
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

impl SubscriberDef for Audit {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("audit")
    }

    fn into_handler(self) -> Self {
        self
    }
}

impl Declared for Audit {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Name, (Open, Fixed, Open)>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("audit")).on_failure(
            FailurePolicies::default()
                .with_panic(FailurePolicy::Skip)
                .with_decode(FailurePolicy::Skip),
        )
    }
}
// --8<-- [end:skip]

fn app() -> RustStream {
    RustStream::new(AppInfo::new("failure-policy", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(Process);
        b.include(Ingest);
        b.include(Audit);
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
