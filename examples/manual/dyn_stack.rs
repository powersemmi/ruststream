//! The service of `examples/dyn_stack.rs` written without the `macros` feature. The middleware
//! is the same code in both forms; only the handler and its mounting differ.
//!
//! ```text
//! cargo test --example manual_dyn_stack --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::pin::Pin;

use ruststream::memory::prelude::*;
use ruststream::runtime::{DynMiddleware, Next};
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

/// The headers of a delivery that names its tenant.
fn tenant(name: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("tenant", name);
    headers
}

// --8<-- [start:middleware]
/// Drops the deliveries of one tenant.
struct Block {
    tenant: String,
}

impl<I: Send + Sync> DynMiddleware<I> for Block {
    fn handle<'a>(
        &'a self,
        input: &'a I,
        ctx: &'a mut Context<'_>,
        next: Next<'a, I>,
    ) -> Pin<Box<dyn Future<Output = HandlerOutcome> + Send + 'a>> {
        Box::pin(async move {
            if ctx.headers().get("tenant") == Some(self.tenant.as_bytes()) {
                tracing::warn!(subscription = ctx.name(), tenant = %self.tenant, "blocked");
                return HandlerOutcome::drop();
            }
            next.run(input, ctx).await
        })
    }
}
// --8<-- [end:middleware]

mod service {
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;

    use ruststream::memory::MemoryMessage;
    use ruststream::memory::prelude::*;
    use ruststream::runtime::{DynMiddleware, DynStack};
    use ruststream::testing::{Outcome, TestApp};

    use super::{Block, Order, tenant};

    // --8<-- [start:app]
    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    /// The chain comes from configuration: one `Block` per tenant the operator listed.
    fn app(blocked: &[&str]) -> impl App {
        let chain = blocked.iter().map(|tenant| {
            let block = Block {
                tenant: (*tenant).to_owned(),
            };
            Arc::new(block) as Arc<dyn DynMiddleware<MemoryMessage>>
        });
        let stack = DynStack::new(chain);
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // The stack wraps one registration, which sees the broker's own message.
            b.include_router(
                Router::new()
                    .include(subscriber("orders", Accept).build())
                    .layer(stack),
            );
        })
    }
    // --8<-- [end:app]

    #[tokio::test]
    async fn a_blocked_tenant_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(&["globex"])).await?;
        let broker = tb.broker::<MemoryBroker>();
        let order = Order { id: 7 };
        for name in ["acme", "globex"] {
            broker
                .message(&order)
                .with_headers(tenant(name))
                .to("orders")
                .publish()
                .await?;
        }

        let outcomes = broker.subscriber("orders").outcomes();
        assert_eq!(outcomes, [Outcome::Ack, Outcome::Drop]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
