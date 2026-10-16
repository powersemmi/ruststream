//! The service of `examples/from_context.rs` written without the `macros` feature: the extractor
//! is the same `FromContext` impl, and the handler resolves it at the top of its body, as the
//! attribute would.
//!
//! ```text
//! cargo test --example manual_from_context --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the test that uses it exists only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
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

// --8<-- [start:extractor]
/// The tenant an order belongs to, read off its `x-tenant` header.
struct Tenant(String);

impl<C: Send, S: Sync> FromContext<C, S> for Tenant {
    type Rejection = HandlerOutcome;

    fn from_context(
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self, HandlerOutcome>> + Send {
        let tenant = ctx.headers().get("x-tenant");
        let tenant = tenant.map(|tenant| Self(String::from_utf8_lossy(tenant).into_owned()));
        // No tenant, no work: the delivery is dropped and the body does not run.
        ready(tenant.ok_or_else(HandlerOutcome::drop))
    }
}

/// An accepted order and its tenant, published on `accepted`.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Accepted {
    order: u64,
    tenant: String,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "accepted")]` writes.
impl OutgoingDestination for Accepted {
    type Form = FixedName;

    const DESTINATION: &'static str = "accepted";
}

impl MessageHeaders for Accepted {
    type Contract = NoHeaders;
}

/// The body resolves the tenant first; a rejection settles the delivery in its place.
struct Accept;

impl Handle<Order, Accepted> for Accept {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> Result<Accepted, HandlerOutcome> {
        let Tenant(tenant) = Tenant::from_context(ctx).await?;
        Ok(Accepted {
            order: order.id,
            tenant,
        })
    }
}
// --8<-- [end:extractor]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Accept).reply().build());
    })
}

// --8<-- [start:test]
#[tokio::test]
async fn an_order_without_a_tenant_is_dropped() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let broker = tb.broker::<MemoryBroker>();
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", "acme");
    broker
        .message(&Order { id: 7 })
        .to("orders")
        .with_headers(headers)
        .publish()
        .await?;
    broker
        .message(&Order { id: 8 })
        .to("orders")
        .publish()
        .await?;

    // The second order never reached the body: the extractor settled it.
    broker
        .subscriber("orders")
        .assert_called(2)
        .settled(HandlerOutcome::drop());
    broker
        .published::<Accepted>("accepted")
        .assert_called_once()
        .with(&Accepted {
            order: 7,
            tenant: "acme".to_owned(),
        });
    Ok(())
}
// --8<-- [end:test]

// The scenario is the test above; the command in the header runs it.
fn main() {}
