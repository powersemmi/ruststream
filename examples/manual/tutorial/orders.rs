//! The tutorial's message types and handlers, written without the `macros` feature: a plain
//! `#[subscriber]` becomes a named type with an `impl Handler`, and the reply form becomes the
//! definition pair the mount site attaches a publisher to.

// --8<-- [start:order]
use std::future::{Future, ready};

use ruststream::prelude::*;
use ruststream::runtime::{Handler, Settle};
use ruststream::schemars::JsonSchema;
use serde::Deserialize;

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) quantity: u32,
}

/// The definition value: `#[subscriber("orders")]` generates this struct and this impl. The
/// subject it carried is named where the handler is registered instead.
pub(crate) struct Handle;

impl Handler<Order> for Handle {
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("order {} x{}", order.id, order.quantity);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:order]

// --8<-- [start:confirm]
use std::any::type_name;

use ruststream::runtime::{
    AllOpen, Declared, Decoded, OutgoingMessageMetadata, PublishingCall, PublishingDef,
    SubscriberBuilder, forms,
};
use ruststream::schemars::schema_for;
use serde::Serialize;

/// The service's answer to an order.
#[derive(Debug, Serialize, JsonSchema)]
pub(crate) struct Confirmation {
    pub(crate) id: u64,
    pub(crate) accepted: bool,
}

/// The reply form the attribute writes out for `publish("confirmations")`: the handler produces
/// the reply and the mount site supplies the publisher it leaves through, so the definition is
/// split into the metadata (`PublishingDef`), the body (`PublishingCall`), and the declaration
/// `include` mounts from (`Declared`).
pub(crate) struct Confirm;

impl Declared for Confirm {
    type Form = forms::Publishing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    // The subject from the attribute, and every mount-site setting (`.workers`, `.on_failure`,
    // `.start_at`) still open.
    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("orders"))
    }
}

impl PublishingDef for Confirm {
    type Input = Decoded<Order>;
    type Injections = ();
    type Reply = Confirmation;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("orders")
    }

    fn reply_name(&self) -> &'static str {
        "confirmations"
    }

    // What the attribute probes off the types for the AsyncAPI document: the input schema, and
    // the reply as a `send` operation on its own channel.
    fn input_schema(&self) -> Option<String> {
        Some(schema_for!(Order).as_value().to_string())
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new("confirmations", type_name::<Confirmation>())
                .with_payload_schema(Some(schema_for!(Confirmation).as_value().to_string())),
        ]
    }
}

impl<State: Send + Sync> PublishingCall<State> for Confirm {
    fn call(
        &self,
        order: &Order,
        _injections: &(),
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}
// --8<-- [end:confirm]
