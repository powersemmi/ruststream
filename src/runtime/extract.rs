//! Handler extractor parameters: [`FromContext`] resolves a value from the per-delivery [`Context`]
//! before the body runs, and [`State`] (backed by [`FromRef`]) pulls a sub-value out of the shared
//! application state.
//!
//! A `#[subscriber]` handler takes the decoded message and an optional `&mut Context`. Any further
//! parameter whose type implements `FromContext` is an extractor: the generated handler resolves it
//! from the delivery context (and the shared state) and binds it before running the body, so
//! dependencies arrive as arguments instead of being reached for through `ctx.state()`. A failed
//! extraction short-circuits the delivery with the rejection's [`HandlerResult`].

use std::convert::Infallible;
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
/// To inject a piece of the application state, use [`State<T>`](State): it implements `FromContext`
/// for any `T` the state can produce (`T: FromRef<S>`), so handlers take `State<T>` without a
/// hand-written impl. Implement `FromContext` directly only for a custom extractor (an auth guard, a
/// request-scoped resolver) that does more than read the state.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{Context, FromContext, HandlerResult};
///
/// // A custom extractor: reject the delivery unless a header is present.
/// struct RequireToken(Vec<u8>);
///
/// impl<C: Send, S: Sync> FromContext<C, S> for RequireToken {
///     type Rejection = HandlerResult;
///     async fn from_context(ctx: &mut Context<'_, C, S>) -> Result<Self, HandlerResult> {
///         match ctx.headers().get("authorization") {
///             Some(token) => Ok(RequireToken(token.to_vec())),
///             None => Err(HandlerResult::drop()),
///         }
///     }
/// }
/// ```
pub trait FromContext<C = (), S = ()>: Sized {
    /// The error returned when extraction fails. It is converted into a [`HandlerResult`] (the
    /// reflexive conversion makes `HandlerResult` itself a valid rejection, and `Infallible` works
    /// for an extractor that never fails) and the delivery is settled by that outcome, skipping the
    /// handler body.
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

/// Produces a value from a reference to the shared application state `S`.
///
/// It is the bridge [`State<T>`](State) uses to pull a sub-value out of the state: a handler taking
/// `State<T>` resolves when `T: FromRef<S>`. Derive it on the state struct with
/// [`FromRef`](macro@crate::FromRef) to get an impl per field (each cloning that field), or
/// implement it by hand to derive a value from several fields.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::FromRef;
///
/// #[derive(Clone)]
/// struct Db;
///
/// struct AppState {
///     db: Db,
/// }
///
/// // What `#[derive(FromRef)]` generates for each field.
/// impl FromRef<AppState> for Db {
///     fn from_ref(state: &AppState) -> Db {
///         state.db.clone()
///     }
/// }
/// ```
pub trait FromRef<S>: Sized {
    /// Produces the value from a reference to the state.
    fn from_ref(state: &S) -> Self;
}

/// Extractor that injects a piece of the shared application state into a handler.
///
/// `State<T>` resolves through [`FromContext`] whenever `T: FromRef<S>`, so a handler can take
/// `State<T>` for any state component - including types defined in other crates (a broker publisher,
/// a client pool), which a per-field `FromContext` impl could not cover under the orphan rule.
/// Derive [`FromRef`](macro@crate::FromRef) on the state to make every field available.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::State;
/// use ruststream::FromRef;
///
/// #[derive(Clone)]
/// struct Orders;
///
/// // Deriving `FromRef` lets handlers take `State<Orders>` (and `State<T>` for any other field).
/// #[derive(FromRef)]
/// struct AppState {
///     orders: Orders,
/// }
///
/// // In a handler: `async fn handle(msg: &M, State(orders): State<Orders>) -> HandlerResult`.
/// let _ = State(Orders);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct State<T>(pub T);

impl<C, S, T> FromContext<C, S> for State<T>
where
    T: FromRef<S> + Send,
{
    type Rejection = Infallible;
    fn from_context(
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let value = T::from_ref(ctx.state());
        async move { Ok(Self(value)) }
    }
}
