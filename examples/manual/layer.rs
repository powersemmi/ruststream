//! The services of `examples/layer.rs` written without the `macros` feature. The layer is the
//! same code in both forms; only the handlers it wraps and their mounting differ.
//!
//! ```text
//! cargo test --example manual_layer --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;

use ruststream::memory::prelude::*;
use ruststream::runtime::{BlanketLayer, Handler, Layer};
use serde::{Deserialize, Serialize};

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

/// One body for every subscription below: it logs the channel and acks.
struct Log;

impl Handle<Order> for Log {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, subscription = ctx.name(), "handled");
        ready(Ok(()))
    }
}

/// The headers of a delivery that names its tenant.
fn tenant(name: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("tenant", name);
    headers
}

// --8<-- [start:layer]
/// Drops a delivery that names no tenant before the handler sees it.
#[derive(Clone)]
struct RequireTenant;

/// The handler `RequireTenant` makes of the one it wraps.
struct Tenanted<H>(H);

impl<H> Layer<H> for RequireTenant {
    type Handler = Tenanted<H>;

    fn layer(&self, inner: H) -> Tenanted<H> {
        Tenanted(inner)
    }
}

impl<M, C, S, H> Handler<M, C, S> for Tenanted<H>
where
    M: Send + Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        if ctx.headers().get("tenant").is_none() {
            tracing::warn!(subscription = ctx.name(), "no tenant header, dropped");
            return HandlerOutcome::drop();
        }
        self.0.handle(msg, ctx).await
    }
}

// The app and a router wrap handlers whose types they hide, through this generic method.
impl BlanketLayer for RequireTenant {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Tenanted(handler)
    }
}
// --8<-- [end:layer]

mod app_wide {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::{Log, Order, RequireTenant, tenant};

    // --8<-- [start:app]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            // Every handler the app mounts below runs inside the layer.
            .layer(RequireTenant)
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("orders", Log).build());
            })
    }
    // --8<-- [end:app]

    #[tokio::test]
    async fn an_order_without_a_tenant_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let order = Order { id: 7 };
        broker.message(&order).to("orders").publish().await?;
        broker
            .message(&order)
            .with_headers(tenant("acme"))
            .to("orders")
            .publish()
            .await?;

        let outcomes = broker.subscriber("orders").outcomes();
        assert_eq!(outcomes, [Outcome::Drop, Outcome::Ack]);
        Ok(())
    }
}

mod scopes {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Log, Order, RequireTenant};

    // --8<-- [start:scopes]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // Before the first `include`: every handler of this router.
            b.include_router(
                Router::new()
                    .layer(RequireTenant)
                    .include(subscriber("refunds", Log).build()),
            );
            // After an `include`: that registration alone; `audit` runs unwrapped.
            b.include_router(
                Router::new()
                    .include(subscriber("invoices", Log).build())
                    .layer(RequireTenant)
                    .include(subscriber("audit", Log).build()),
            );
        })
    }
    // --8<-- [end:scopes]

    #[tokio::test]
    async fn only_the_wrapped_handlers_drop_an_untenanted_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let order = Order { id: 7 };
        for channel in ["refunds", "invoices", "audit"] {
            broker.message(&order).to(channel).publish().await?;
        }

        broker.subscriber("refunds").settled(HandlerOutcome::drop());
        broker
            .subscriber("invoices")
            .settled(HandlerOutcome::drop());
        broker.subscriber("audit").settled(HandlerOutcome::ack());
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
