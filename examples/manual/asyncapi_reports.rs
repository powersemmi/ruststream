//! The service of `examples/asyncapi_reports.rs` written without the `macros` feature. What the
//! derives declare is written out by hand: each outgoing type's destination and header contract,
//! and the slot's dictionary with the entries the document reports. The tests are the ones of the
//! macro form, on the same document.
//!
//! ```text
//! cargo test --example manual_asyncapi_reports --no-default-features --features memory,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::any::type_name;
use std::fmt::Display;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::runtime::{
    BoundSegment, Documentable, MissingSegment, PublishAt, PublishError, TemplateAddress,
};
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
#[derive(Debug, Serialize, JsonSchema)]
struct Confirmed {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the chain names the destination.
impl OutgoingDestination for Confirmed {
    type Form = CallerName;
}

impl MessageHeaders for Confirmed {
    type Contract = NoHeaders;
}

struct Confirm;

impl Handle<Order, Confirmed> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmed, HandlerOutcome>> {
        ready(Ok(Confirmed { id: order.id }))
    }
}
// --8<-- [end:reply]

/// The `tenant` placeholder, named in the compile error of a publish that never bound it.
#[derive(Debug, Clone, Copy)]
struct Tenant;

/// The setters of `orders.{tenant}.accepted`: `publish` exists once every placeholder is bound.
#[must_use = "an address builder does nothing until publish() is awaited"]
struct AcceptedAt<Cont, S0> {
    cont: Cont,
    tenant: S0,
}

impl<Cont> AcceptedAt<Cont, MissingSegment<Tenant>> {
    fn tenant(self, value: impl Display) -> AcceptedAt<Cont, String> {
        AcceptedAt {
            cont: self.cont,
            tenant: value.to_string(),
        }
    }
}

impl<Cont, S0> AcceptedAt<Cont, S0> {
    async fn publish(self) -> Result<(), PublishError<<Cont as PublishAt>::Error>>
    where
        Cont: PublishAt,
        S0: BoundSegment,
    {
        let address = format!("orders.{}.accepted", self.tenant);
        PublishAt::publish_at(self.cont, &address).await
    }
}

// --8<-- [start:parameters]
/// An order accepted for one tenant: the tenant is a segment of the channel's name.
#[derive(Debug, Serialize, JsonSchema)]
struct Accepted {
    id: u64,
}

impl OutgoingDestination for Accepted {
    type Form = NameTemplate;

    const DESTINATION: &'static str = "orders.{tenant}.accepted";
    const PARAMETERS: &'static [&'static str] = &["tenant"];
}

impl MessageHeaders for Accepted {
    type Contract = NoHeaders;
}

impl<Cont> TemplateAddress<Cont> for Accepted {
    type Builder = AcceptedAt<Cont, MissingSegment<Tenant>>;

    fn begin(cont: Cont) -> Self::Builder {
        AcceptedAt {
            cont,
            tenant: MissingSegment::new(),
        }
    }
}

/// The slot the handler publishes through, and the dictionary the document reports for it.
struct Tenants;

impl OutSlot for Tenants {
    const NAME: &'static str = "Tenants";
    type Destination = Reads;

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        <Accepted as OutMessages<Self>>::outgoing()
    }
}

impl PublishedThrough<Tenants> for Accepted {}

impl<M: OutSlot> OutMessages<M> for Accepted {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(Self::DESTINATION, type_name::<Self>())
                .with_parameters(Self::PARAMETERS)
                .with_payload_schema(<Self as Documentable>::schema_json()),
        ]
    }
}

struct Accept;

impl<O> Handle<Order, (), Outs<(O,)>> for Accept
where
    O: OutEntry<Tenants, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(O,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let accepted = Accepted { id: order.id };
        let tenants = outs.get(Tenants);
        match tenants
            .message(&accepted)
            .to()
            .tenant(&order.tenant)
            .publish()
            .await
        {
            Ok(()) => Ok(()),
            Err(_) => Err(HandlerOutcome::retry()),
        }
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

/// The pair input carries the header contract, so the registration reports its schema.
struct Ship;

impl Handle<Message<ShipmentMeta, Shipment>> for Ship {
    fn handle(
        &self,
        shipment: &Message<ShipmentMeta, Shipment>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let meta = &shipment.headers;
        tracing::info!(shipment.body.id, trace_id = %meta.trace_id, meta.attempt, "shipped");
        ready(Ok(()))
    }
}
// --8<-- [end:headers]

/// A payment to charge.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Payment {
    id: u64,
}

struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::warn!(payment.id, "gateway refused the charge");
        ready(Err(HandlerOutcome::retry()))
    }
}

// --8<-- [start:retry]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(
            subscriber("order-requests", Confirm)
                .describe("Confirms every order request.")
                .reply()
                .to("confirmations")
                .build(),
        );
        b.include(subscriber("orders", Accept).build())
            .out(Tenants, Publish)
            .build();
        b.include(subscriber("shipments", Ship).build());
        // The cap and the dead-letter destination reach the document with the registration.
        b.include(subscriber("payments", Charge).build())
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
