//! What the AsyncAPI document reports beyond a subscriber's channel: the reply of a request, the
//! parameters of a name template, a header contract, and the retry declaration of a
//! registration. The tests check what the document says.
//!
//! ```text
//! cargo test --example asyncapi_reports --features macros,memory,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:reply]
/// The confirmation an order request gets back.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
struct Confirmed {
    id: u64,
}

/// Confirms every order request.
#[subscriber("order-requests", reply("confirmations"))]
async fn confirm(order: &Order) -> Confirmed {
    Confirmed { id: order.id }
}
// --8<-- [end:reply]

// --8<-- [start:parameters]
/// An order accepted for one tenant: the tenant is a segment of the channel's name.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders.{tenant}.accepted")]
struct Accepted {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Accepted)]
struct Tenants;

#[subscriber("orders")]
async fn accept(order: &Order, Out(tenants): Out<impl Publisher, Tenants>) -> HandlerOutcome {
    let accepted = Accepted { id: order.id };
    match tenants
        .message(&accepted)
        .to()
        .tenant(&order.tenant)
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:parameters]

// --8<-- [start:headers]
/// What travels in the headers of a shipment.
#[derive(Debug, Deserialize, JsonSchema)]
struct ShipmentMeta {
    /// The trace the shipment belongs to.
    trace_id: String,
    /// The carrier's attempt number.
    attempt: u32,
}

/// A parcel handed to the carrier.
#[derive(Debug, Deserialize, JsonSchema)]
struct Shipment {
    id: u64,
}

#[subscriber("shipments")]
async fn ship(shipment: &Shipment, Headers(meta): Headers<ShipmentMeta>) -> HandlerOutcome {
    tracing::info!(shipment.id, trace_id = %meta.trace_id, meta.attempt, "shipped");
    HandlerOutcome::ack()
}
// --8<-- [end:headers]

/// A payment to charge.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Payment {
    id: u64,
}

#[subscriber("payments")]
async fn charge(payment: &Payment) -> HandlerOutcome {
    tracing::warn!(payment.id, "gateway refused the charge");
    HandlerOutcome::retry()
}

// --8<-- [start:retry]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm);
        b.include(accept).out(Tenants, Publish).build();
        b.include(ship);
        // The cap and the dead-letter destination reach the document with the registration.
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("payments.dead");
    })
}
// --8<-- [end:retry]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::app;

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&build_spec(&app()).to_json()?)?)
    }

    #[test]
    fn the_reply_rides_the_receive_operation() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let operation = &spec["operations"]["receive_order_requests"];

        assert_eq!(
            operation["reply"],
            json!({
                "channel": { "$ref": "#/channels/confirmations" },
                "messages": [{ "$ref": "#/channels/confirmations/messages/Confirmed" }],
            })
        );
        assert_eq!(
            spec["channels"]["confirmations"]["address"],
            "confirmations"
        );
        assert!(spec["components"]["messages"]["Confirmed"]["payload"].is_object());
        // A reply is the request's operation, never a `send` of its own.
        assert!(spec["operations"]["send_order_requests_confirmations"].is_null());
        Ok(())
    }

    #[test]
    fn a_name_template_becomes_a_channel_with_parameters() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channel = &spec["channels"]["orders.{tenant}.accepted"];

        assert_eq!(channel["address"], "orders.{tenant}.accepted");
        assert_eq!(channel["parameters"], json!({ "tenant": {} }));
        let send = &spec["operations"]["send_orders_orders__tenant__accepted"];
        assert_eq!(send["action"], "send");
        assert_eq!(
            send["messages"],
            json!([{ "$ref": "#/channels/orders.{tenant}.accepted/messages/Accepted" }])
        );
        Ok(())
    }

    #[test]
    fn the_header_contract_becomes_the_headers_schema() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let headers = &spec["components"]["messages"]["Shipment"]["headers"];

        assert_eq!(headers["required"], json!(["trace_id", "attempt"]));
        assert_eq!(
            headers["properties"]["trace_id"]["description"],
            "The trace the shipment belongs to."
        );
        Ok(())
    }

    #[test]
    fn the_retry_declaration_is_an_extension_of_the_operation() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["operations"]["receive_payments"]["x-ruststream-retry"],
            json!({ "maxAttempts": 3, "deadLetter": "payments.dead" })
        );
        assert_eq!(
            spec["channels"]["payments.dead"]["address"],
            "payments.dead"
        );
        assert_eq!(
            spec["operations"]["send_payments_payments_dead"]["action"],
            "send"
        );
        Ok(())
    }
}
