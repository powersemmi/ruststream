//! [`FromContext`]: typed values a handler declares as parameters, resolved from the per-delivery
//! [`Context`] before the body runs.
//!
//! A `#[subscriber]` handler takes the decoded message and an optional `&mut Context`. Any further
//! parameter whose type implements `FromContext` is an extractor: the generated handler resolves it
//! from the delivery context (and the shared state) and binds it before running the body, so
//! dependencies arrive as arguments instead of being reached for through `ctx.state()`. A failed
//! extraction short-circuits the delivery with the rejection's [`HandlerResult`].

use std::future::Future;

use super::context::Context;
use super::handler::HandlerResult;

/// A value resolved from the per-delivery [`Context`] and shared state, ready to be passed to a
/// handler as a parameter.
///
/// When a type implements it, `#[subscriber]` handlers can take that type as an argument: the
/// generated handler calls [`from_context`](Self::from_context) for each such parameter, in
/// declaration order, before the body runs. Resolution is async so it may do work (a lookup, a
/// scoped allocation) and fallible so it may reject the delivery; the [`Rejection`](Self::Rejection)
/// is turned into a [`HandlerResult`] that settles the message (typically a nack).
///
/// The first handler parameter (the message `&M`) and the optional `&mut Context` are not
/// extractors; every other by-value parameter is.
///
/// Usually you do not implement this trait by hand. Deriving `FromState` on the application state
/// generates an impl for each of its fields, so handlers take the field types as arguments directly.
/// Implement it yourself only for a custom extractor (an auth guard, a request-scoped resolver) that
/// does more than clone a field out of the state.
///
/// # Examples
///
/// ```
/// use ruststream::FromState;
///
/// // A cheaply cloned dependency handlers should receive directly.
/// #[derive(Clone)]
/// struct Orders;
///
/// // Deriving `FromState` makes each field an extractor: a handler can now take `orders: Orders`.
/// #[derive(FromState)]
/// struct AppState {
///     orders: Orders,
/// }
/// ```
pub trait FromContext<C = (), S = ()>: Sized {
    /// The error returned when extraction fails. It is converted into a [`HandlerResult`] (the
    /// reflexive conversion makes `HandlerResult` itself a valid rejection) and the delivery is
    /// settled by that outcome, skipping the handler body.
    type Rejection: Into<HandlerResult>;

    /// Resolves the value from the delivery context. The context is borrowed mutably so an
    /// extractor may also read broker fields or take scratch a middleware left for it.
    ///
    /// # Errors
    ///
    /// Returns [`Rejection`](Self::Rejection) when the value cannot be produced; the dispatcher
    /// settles the delivery by the resulting [`HandlerResult`] instead of running the handler.
    fn from_context(
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}
