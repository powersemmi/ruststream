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
/// Implement it for a type to let `#[subscriber]` handlers take that type as an argument: the
/// generated handler calls [`from_context`](Self::from_context) for each such parameter, in
/// declaration order, before the body runs. Resolution is async so it may do work (a lookup, a
/// scoped allocation) and fallible so it may reject the delivery; the [`Rejection`](Self::Rejection)
/// is turned into a [`HandlerResult`] that settles the message (typically a nack).
///
/// The first handler parameter (the message `&M`) and the optional `&mut Context` are not
/// extractors; every other by-value parameter is.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{Context, FromContext, HandlerResult};
///
/// // A cheaply cloned service we want handlers to receive directly.
/// #[derive(Clone)]
/// struct Service;
///
/// // The extractor that produces it. Implemented for any context `C` and state `S`.
/// struct UseService(Service);
///
/// impl<C: Send, S: Sync> FromContext<C, S> for UseService {
///     type Rejection = HandlerResult;
///     async fn from_context(_ctx: &mut Context<'_, C, S>) -> Result<Self, HandlerResult> {
///         Ok(UseService(Service))
///     }
/// }
///
/// // The generated handler resolves it from the delivery context before running the body.
/// async fn resolve<C: Send, S: Sync>(ctx: &mut Context<'_, C, S>) -> Result<(), HandlerResult> {
///     let UseService(_service) = UseService::from_context(ctx).await?;
///     Ok(())
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
