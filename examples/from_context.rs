//! A custom extractor: a type of the service's own that `FromContext` resolves from the
//! per-delivery context before the handler body runs, rejecting the delivery when it cannot.
//! The scenario is a test.
//!
//! ```text
//! cargo test --example from_context --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the test that uses it exists only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
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
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "accepted")]
struct Accepted {
    order: u64,
    tenant: String,
}

/// The tenant arrives as a parameter, resolved before the body runs.
#[subscriber("orders", reply)]
async fn accept(order: &Order, Tenant(tenant): Tenant) -> Accepted {
    Accepted {
        order: order.id,
        tenant,
    }
}
// --8<-- [end:extractor]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(accept);
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
