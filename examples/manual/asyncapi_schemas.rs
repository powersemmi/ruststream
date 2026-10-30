//! The service of `examples/asyncapi_schemas.rs` written without the `macros` feature. A
//! `subscriber(..)` registration is documented by default and demands the `JsonSchema` derive;
//! `.undocumented()` lifts that demand from one registration. The tests are the ones of the macro
//! form, on the same document.
//!
//! ```text
//! cargo test --example manual_asyncapi_schemas --no-default-features --features memory,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;

use ruststream::asyncapi::Spec;
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::Deserialize;

// --8<-- [start:schema]
/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    /// The order number, unique per shop.
    id: u64,
    /// What was ordered.
    item: String,
    /// How many items; one when absent.
    quantity: Option<u32>,
}

/// The handler `#[subscriber("orders")]` writes for the body.
struct Accept;

impl Handle<Order> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, item = %order.item, quantity = ?order.quantity, "accepted");
        ready(Ok(()))
    }
}
// --8<-- [end:schema]

// --8<-- [start:message_info]
/// A refund a customer asked for.
// The schema's title names the component: the rename keeps the name the contract was published
// under, whatever the type is called.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(rename = "RefundRequest")]
struct RefundRequestV2 {
    order_id: u64,
}

struct Refund;

impl Handle<RefundRequestV2> for Refund {
    fn handle(
        &self,
        request: &RefundRequestV2,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(request.order_id, "refund requested");
        ready(Ok(()))
    }
}
// --8<-- [end:message_info]

// --8<-- [start:gaps]
/// An audit record another team publishes, with no schema shipped for it.
#[derive(Debug, Deserialize)]
struct AuditRecord {
    action: String,
}

struct Record;

impl Handle<AuditRecord> for Record {
    fn handle(
        &self,
        entry: &AuditRecord,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(action = %entry.action, "audited");
        ready(Ok(()))
    }
}

/// The release check: every message in the document has a schema, or the check names the ones
/// that do not.
fn check_schemas(spec: &Spec) -> Result<(), String> {
    match spec.messages_without_schema().as_slice() {
        [] => Ok(()),
        gaps => Err(format!("no payload schema for {}", gaps.join(", "))),
    }
}
// --8<-- [end:gaps]

// --8<-- [start:undocumented]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Accept).build())
            .include(subscriber("refunds", Refund).build())
            // `AuditRecord` has no `JsonSchema`: without this step the mount does not compile.
            .include(subscriber("audit", Record).undocumented().build())
    })
}
// --8<-- [end:undocumented]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{app, check_schemas};

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&build_spec(&app()).to_json()?)?)
    }

    #[test]
    fn the_derive_gives_the_payload_its_schema() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let order = &spec["components"]["messages"]["Order"];

        assert_eq!(order["description"], "An order placed by a customer.");
        let payload = &order["payload"];
        assert_eq!(
            payload["$schema"],
            "http://json-schema.org/draft-07/schema#"
        );
        assert_eq!(payload["required"], json!(["id", "item"]));
        assert_eq!(
            payload["properties"]["id"]["description"],
            "The order number, unique per shop."
        );
        assert_eq!(
            payload["properties"]["quantity"]["type"],
            json!(["integer", "null"])
        );
        Ok(())
    }

    #[test]
    fn the_component_keeps_its_published_name() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let refund = &spec["components"]["messages"]["RefundRequest"];

        assert_eq!(refund["name"], "RefundRequest");
        assert_eq!(refund["description"], "A refund a customer asked for.");
        assert!(refund["payload"].is_object());
        assert_eq!(
            spec["channels"]["refunds"]["messages"]["RefundRequest"]["$ref"],
            "#/components/messages/RefundRequest"
        );
        Ok(())
    }

    #[test]
    fn the_check_names_the_message_without_a_schema() -> Result<(), Box<dyn Error>> {
        let spec = build_spec(&app());

        assert_eq!(spec.messages_without_schema(), ["AuditRecord"]);
        assert_eq!(
            check_schemas(&spec),
            Err("no payload schema for AuditRecord".to_owned())
        );
        // The message is still in the document, with its channel; only the schema is missing.
        let document = document()?;
        assert_eq!(document["channels"]["audit"]["address"], "audit");
        assert!(document["components"]["messages"]["AuditRecord"]["payload"].is_null());
        Ok(())
    }
}
