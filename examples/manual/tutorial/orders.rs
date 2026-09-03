//! The tutorial's message types and handlers, written without the `macros` feature: a plain
//! `#[subscriber]` becomes a named type with an `impl Handle`, and the reply form is the same
//! trait with its reply type filled in - the mount site binds each to its subject.

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
pub(crate) struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("order {} x{}", order.id, order.quantity);
        ready(Ok(()))
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

/// The reply form of the same trait: the second parameter of `Handle` is the reply type, so the
/// body returns a `Confirmation`, and the mount site names the destination and supplies the
/// publisher it leaves through.
pub(crate) struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}
// --8<-- [end:confirm]
