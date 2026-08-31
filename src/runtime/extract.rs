//! Handler extractor parameters: [`FromContext`] resolves a value from the per-delivery [`Context`]
//! before the body runs, and [`State`] (backed by [`FromRef`]) pulls a sub-value out of the shared
//! application state.
//!
//! A `#[subscriber]` handler takes the decoded message and an optional `&mut Context`. Any further
//! parameter whose type implements `FromContext` is an extractor: the generated handler resolves it
//! from the delivery context (and the shared state) and binds it before running the body, so
//! dependencies arrive as arguments instead of being reached for through `ctx.state()`. A failed
//! extraction short-circuits the delivery with the rejection's [`HandlerOutcome`].

use std::any::type_name;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;

use serde::de::DeserializeOwned;
use tracing::warn;

use crate::ContextField;

use super::context::Context;
use super::failure::FailurePolicy;
use super::handler::HandlerOutcome;

/// A value resolved from the per-delivery [`Context`] and shared state, ready to be passed to a
/// handler as a parameter.
///
/// When a type implements it, `#[subscriber]` handlers can take that type as an argument: the
/// generated handler calls [`from_context`](Self::from_context) for each such parameter, in
/// declaration order, before the body runs. Resolution is async so it may do work (a lookup, a
/// scoped allocation) and fallible so it may reject the delivery; the [`Rejection`](Self::Rejection)
/// is turned into a [`HandlerOutcome`] that settles the message (typically a nack).
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
/// use ruststream::runtime::{Context, FromContext, HandlerOutcome};
///
/// // A custom extractor: reject the delivery unless a header is present.
/// struct RequireToken(Vec<u8>);
///
/// impl<C: Send, S: Sync> FromContext<C, S> for RequireToken {
///     type Rejection = HandlerOutcome;
///     async fn from_context(ctx: &mut Context<'_, C, S>) -> Result<Self, HandlerOutcome> {
///         match ctx.headers().get("authorization") {
///             Some(token) => Ok(RequireToken(token.to_vec())),
///             None => Err(HandlerOutcome::drop()),
///         }
///     }
/// }
/// ```
pub trait FromContext<C = (), S = ()>: Sized {
    /// The error returned when extraction fails. It is converted into a [`HandlerOutcome`] (the
    /// reflexive conversion makes `HandlerOutcome` itself a valid rejection, and `Infallible`
    /// works for an extractor that never fails) and the delivery is settled by that outcome,
    /// skipping the handler body.
    type Rejection: Into<HandlerOutcome>;

    /// Resolves the value from the delivery context. The context is borrowed mutably so an
    /// extractor may also read broker fields or take scratch a middleware left for it.
    ///
    /// # Errors
    ///
    /// Returns [`Rejection`](Self::Rejection) when the value cannot be produced; the dispatcher
    /// settles the delivery by the resulting [`HandlerOutcome`] instead of running the handler.
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
/// # #[cfg(feature = "macros")]
/// # {
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
/// # }
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

/// Extractor that injects one broker context field into a handler, by its key.
///
/// Where [`State<T>`](State) pulls a value out of the shared application state, `Ctx<K>` pulls
/// one field out of the broker's per-delivery context: `Ctx(offset): Ctx<Offset>` binds the
/// value the key `Offset` reads. The key implements [`ContextField`], which names the context
/// type it reads from - so a handler using only `Ctx` extractors needs no `&mut Context`
/// parameter at all: the `#[subscriber]` macro projects the subscription's context type from
/// the first `Ctx` key in the signature. With a `&mut Context<'_, C>` parameter also present,
/// the keys must read that same `C` (the compiler enforces it).
///
/// Values are owned ([`ContextField::Value`] is `'static`): extractors bind before the handler
/// body runs, so borrowing from the context is not an option. Keys yielding borrowed values
/// stay readable through `ctx.context(KEY)`.
///
/// # Examples
///
/// ```
/// use ruststream::ContextField;
/// use ruststream::runtime::Ctx;
///
/// struct Delivery {
///     offset: u64,
/// }
///
/// #[derive(Clone, Copy, Default)]
/// struct Offset;
///
/// impl ContextField for Offset {
///     type Context = Delivery;
///     type Value = u64;
///     fn read(self, src: &Delivery) -> u64 {
///         src.offset
///     }
/// }
///
/// // In a handler: `async fn handle(msg: &M, Ctx(offset): Ctx<Offset>) -> HandlerResult`.
/// let extracted = Ctx::<Offset>(42);
/// assert_eq!(extracted.0, 42);
/// ```
pub struct Ctx<K: ContextField>(pub K::Value);

impl<K: ContextField> fmt::Debug for Ctx<K>
where
    K::Value: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ctx").field(&self.0).finish()
    }
}

impl<S, K> FromContext<K::Context, S> for Ctx<K>
where
    K: ContextField,
    K::Context: Send,
    S: Sync,
{
    type Rejection = Infallible;
    fn from_context(
        ctx: &mut Context<'_, K::Context, S>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let value = K::default().read(ctx.cx_ref());
        async move { Ok(Self(value)) }
    }
}

/// Extractor that parses the delivery headers into a typed contract before the body runs.
///
/// `Headers<T>` parses the header map through the crate-internal typed-headers machinery:
/// `T` is a flat struct whose fields name headers, with string-encoded values parsed into what
/// each field expects. The handler body only runs when the whole contract parsed; a missing or
/// unparsable header settles the delivery by the subscriber's `on_failure(decode = ..)` policy
/// (drop by default) after a `WARN` naming the subscription and the contract type - a header
/// contract violation is the same class of bad external input as a payload that does not decode,
/// so one policy covers both.
///
/// Under the `asyncapi` feature the `#[subscriber]` macro also lifts `T`'s
/// [`schemars::JsonSchema`] into the message's headers schema, so the same declaration feeds the
/// generated document.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{Headers, HandlerOutcome};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct ChunkMeta {
///     task_id: u64,
///     chunk_no: u32,
/// }
///
/// // In a handler:
/// // async fn handle(chunk: &[u8], Headers(meta): Headers<ChunkMeta>) -> HandlerOutcome
/// let Headers(meta) = Headers(ChunkMeta { task_id: 7, chunk_no: 3 });
/// assert_eq!(meta.chunk_no, 3);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Headers<T>(pub T);

impl<T: DeserializeOwned> Headers<T> {
    /// Parses the delivery headers under the given failure policy. The `#[subscriber]` macro
    /// routes generated handlers through here so `on_failure(decode = ..)` applies; the plain
    /// [`FromContext`] impl uses the [`Drop`](FailurePolicy::Drop) default.
    #[doc(hidden)]
    pub fn extract<C, S>(
        ctx: &mut Context<'_, C, S>,
        policy: FailurePolicy,
    ) -> Result<Self, HandlerOutcome> {
        match ctx.headers().to_typed::<T>() {
            Ok(value) => Ok(Self(value)),
            Err(err) => {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %ctx.name(),
                    headers_type = type_name::<T>(),
                    error = %err,
                    "typed header extraction failed",
                );
                #[cfg(any(feature = "testing", feature = "otel"))]
                ctx.mark_decode_failed();
                Err(match policy {
                    FailurePolicy::FailFast => {
                        ctx.fail_fast(&format!("header extraction failed: {err}"));
                        HandlerOutcome::drop()
                    }
                    other => other
                        .settlement()
                        .map_or_else(HandlerOutcome::drop, Into::into),
                })
            }
        }
    }
}

impl<C, S, T> FromContext<C, S> for Headers<T>
where
    T: DeserializeOwned + Send,
    C: Send,
    S: Sync,
{
    type Rejection = HandlerOutcome;
    fn from_context(
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self, HandlerOutcome>> + Send {
        let result = Self::extract(ctx, FailurePolicy::Drop);
        async move { result }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::HeaderMap;
    use crate::runtime::dispatch::Delivery;

    #[derive(Debug, Deserialize)]
    struct Meta {
        task_id: u64,
    }

    #[derive(Default)]
    struct Offset;

    impl ContextField for Offset {
        type Context = u64;
        type Value = u64;

        fn read(self, src: &u64) -> u64 {
            *src
        }
    }

    fn headers_with(task_id: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("task_id", task_id);
        headers
    }

    #[test]
    fn a_satisfied_header_contract_binds_the_typed_value() {
        let state = ();
        let delivery = Delivery::empty();
        let headers = headers_with("7");
        let mut ctx = Context::new("orders", &headers, &state, (), &delivery);

        let Headers(meta) =
            Headers::<Meta>::extract(&mut ctx, FailurePolicy::Drop).expect("contract holds");
        assert_eq!(meta.task_id, 7);
    }

    #[test]
    fn a_violated_contract_settles_by_the_configured_policy() {
        let state = ();
        let delivery = Delivery::empty();
        let headers = headers_with("not a number");

        // Drop is the default: the delivery is settled away rather than requeued forever.
        let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
        assert!(
            Headers::<Meta>::extract(&mut ctx, FailurePolicy::Drop)
                .map(|_| ())
                .unwrap_err()
                .is_drop()
        );

        // Retry keeps the delivery in play, in case the contract violation is transient wiring.
        let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
        assert!(
            Headers::<Meta>::extract(&mut ctx, FailurePolicy::Retry)
                .map(|_| ())
                .unwrap_err()
                .is_retry()
        );

        // Fail-fast still settles the delivery; the service teardown is signalled separately.
        let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
        assert!(
            Headers::<Meta>::extract(&mut ctx, FailurePolicy::FailFast)
                .map(|_| ())
                .unwrap_err()
                .is_drop()
        );
    }

    #[tokio::test]
    async fn the_plain_extractor_path_defaults_to_dropping_the_delivery() {
        let state = ();
        let delivery = Delivery::empty();
        let headers = headers_with("not a number");
        let mut ctx = Context::new("orders", &headers, &state, (), &delivery);

        let outcome = <Headers<Meta> as FromContext<(), ()>>::from_context(&mut ctx).await;
        assert!(outcome.map(|_| ()).unwrap_err().is_drop());
    }

    #[test]
    fn a_context_field_parameter_shows_its_value_when_debugged() {
        // The extractor is what a handler sees, so its Debug has to reach the value itself.
        assert_eq!(format!("{:?}", Ctx::<Offset>(42)), "Ctx(42)");
    }
}
