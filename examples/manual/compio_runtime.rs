//! The service of `examples/compio_runtime.rs` without the `macros` feature: the handlers are
//! named types and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_compio_runtime --no-default-features --features compio,testing,memory,json -- run
//! cargo test --example manual_compio_runtime --no-default-features --features compio,testing,memory,json
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::compio::run_main;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::nonzero;
use ruststream::prelude::SubscriberSettings;
use ruststream::runtime::compio::{App, AppInfo, Core, RustStream};
use ruststream::runtime::{Context, Handle, HandlerOutcome, subscriber};
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation {
            id: order.id,
            core: Core::current().index(),
        }))
    }
}

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "audited");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .logger(TracingLogger)
        .with_broker(LocalMemoryBroker::<4>::new(), |b| {
            b.include(
                subscriber(LocalMemorySource::new("orders"), Confirm)
                    .workers(nonzero!(4))
                    .reply()
                    .to("confirmations")
                    .build(),
            )
            .out_reply(LocalMemoryPublish)
            .include(subscriber(LocalMemorySource::new("orders"), Audit).build())
        })
}
// --8<-- [end:app]

// What `#[ruststream::app(runtime = compio, cores = 4)]` writes.
fn main() -> ExitCode {
    run_main(Some(4), app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Confirmation {
    id: u64,
    core: usize,
}

// The mount site names the reply's destination, `reply().to("confirmations")`.
impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use compio::runtime::Runtime;
    use ruststream::memory::local::LocalMemoryBroker;
    use ruststream::testing::compio::TestApp;

    use super::{Confirmation, Order, app};

    // --8<-- [start:test]
    #[test]
    fn an_order_is_confirmed_and_audited() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            let orders = tb.broker::<LocalMemoryBroker<4>>();
            orders
                .message(&Order { id: 7 })
                .to("orders")
                .publish()
                .await?;

            // The harness runs the app as the one core of the test's runtime.
            orders
                .published::<Confirmation>("confirmations")
                .assert_called_once()
                .with(&Confirmation { id: 7, core: 0 });
            orders.subscriber("orders").assert_called(2);
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:test]
}
