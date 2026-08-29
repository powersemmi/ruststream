//! The tutorial's message types and handlers, written without the `macros` feature: a plain
//! `#[subscriber]` becomes a named type with an `impl Handler`, and the reply form becomes a
//! named type with an `impl Reply` - the mount site binds each to its subject.

// --8<-- [start:order]
use std::future::{Future, ready};

use ruststream::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::Deserialize;

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) quantity: u32,
}

/// The handler: `#[subscriber("orders")]` generates this struct and this impl. The subject it
/// carried is named where the handler is mounted instead.
pub(crate) struct Handle;

impl Handler<Order> for Handle {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("order {} x{}", order.id, order.quantity);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:order]

// --8<-- [start:confirm]
use serde::Serialize;

/// The service's answer to an order.
#[derive(Debug, Serialize, JsonSchema)]
pub(crate) struct Confirmation {
    pub(crate) id: u64,
    pub(crate) accepted: bool,
}

/// The reply body the attribute writes out for `publish("confirmations")`: the handler produces
/// the reply, and the mount site names the destination and supplies the publisher it leaves
/// through.
pub(crate) struct Confirm;

impl Reply<Order> for Confirm {
    type Out = Confirmation;

    fn reply(
        &self,
        order: &Order,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}
// --8<-- [end:confirm]
