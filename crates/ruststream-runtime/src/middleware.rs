//! Middleware infrastructure: [`Layer`] composes wrappers around handlers, tower-style.
//!
//! A `Layer` consumes one handler and returns another. Stacking layers builds the final
//! handler the router invokes. Layers run in the order they are applied: the outermost
//! `with(...)` runs first.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "json")]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//! use ruststream::runtime::layers::TracingLayer;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[subscriber("orders")]
//! async fn accept(order: &Order) -> HandlerOutcome {
//!     tracing::info!(order.id, "accepted");
//!     HandlerOutcome::ack()
//! }
//!
//! #[subscriber("audit")]
//! async fn audit(order: &Order) -> HandlerOutcome {
//!     tracing::info!(order.id, "audited");
//!     HandlerOutcome::ack()
//! }
//!
//! fn app() -> impl App {
//!     // Every handler of the app runs inside the tracing layer's span.
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .layer(TracingLayer::default())
//!         .with_broker(MemoryBroker::new(), |b| {
//!             b.include(accept);
//!             b.include(audit);
//!         })
//! }
//! # }
//! # fn main() {}
//! ```

use super::context::Context;
use super::handler::{Handler, HandlerOutcome, LocalHandler};

/// A function from one handler to another. Apply with [`HandlerExt::with`], or to one
/// registration with the `layer` step of the chain that mounts it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot wrap `{H}`",
    note = "a layer that wraps one registration implements `Layer<H>` for the handler it wraps; \
            a layer that wraps every handler in an app or a router implements `BlanketLayer` \
            instead and rides `RustStream::layer(..)` or `Router::layer(..)`"
)]
pub trait Layer<H> {
    /// The handler type produced by this layer.
    type Handler;

    /// Wrap `inner` and return the composed handler.
    fn layer(&self, inner: H) -> Self::Handler;
}

/// A [`Layer`] that wraps a handler on *any* message type, not one fixed `H`.
///
/// [`Layer`] is checked per concrete handler (`L: Layer<H>`). That bound cannot be discharged when
/// the handler types are hidden, which is exactly the case for a [`Router`](super::Router) mounted
/// through [`include_router`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html#method.include_router): its handlers are erased behind
/// [`RouterDef`](super::RouterDef). `BlanketLayer` carries the wrapping as a generic method, so a
/// layer that applies uniformly (logging, metrics) can wrap every router handler from one bound.
///
/// Implemented for [`Identity`], a [`Stack`] of blanket layers, and the bundled
/// [`TracingLayer`](layers::TracingLayer). Implement it for a custom layer to let the app's global
/// stack reach router handlers; a layer that only wraps specific handler types cannot be blanket.
///
/// # Examples
///
/// A logging layer that wraps every handler of the application:
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{BlanketLayer, Handler, Identity, Layer, Stack};
/// use serde::Deserialize;
///
/// #[derive(Clone)]
/// struct LogLayer;
///
/// struct Logged<H>(H);
///
/// impl<H> Layer<H> for LogLayer {
///     type Handler = Logged<H>;
///
///     fn layer(&self, inner: H) -> Logged<H> {
///         Logged(inner)
///     }
/// }
///
/// impl<M, C, S, H> Handler<M, C, S> for Logged<H>
/// where
///     M: Send + Sync,
///     C: Send,
///     S: Send + Sync,
///     H: Handler<M, C, S>,
/// {
///     async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
///         println!("-> {}", ctx.name());
///         let outcome = self.0.handle(msg, ctx).await;
///         println!("<- {}", ctx.name());
///         outcome
///     }
/// }
///
/// // The application stack wraps handlers whose concrete type the mount site hides.
/// impl BlanketLayer for LogLayer {
///     fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
///     where
///         M: Send + Sync + 'static,
///         C: Send + 'static,
///         S: Send + Sync + 'static,
///         H: Handler<M, C, S> + 'static,
///     {
///         Logged(handler)
///     }
/// }
///
/// # #[derive(Deserialize)]
/// # struct Order {
/// #     id: u64,
/// # }
/// # #[subscriber("orders")]
/// # async fn handle(order: &Order) -> HandlerOutcome {
/// #     HandlerOutcome::ack()
/// # }
/// fn app() -> RustStream<Stack<LogLayer, Identity>> {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(LogLayer)
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(handle);
///         })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait BlanketLayer: Send + Sync {
    /// Wraps `handler`, returning the layered handler. `S` is the app's shared-state type, threaded
    /// so a blanket layer wraps a router handler without fixing its state type.
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static;
}

/// Convenience extension trait for fluent layer stacking on any [`Handler`].
pub trait HandlerExt<M, C = (), S = ()>: Handler<M, C, S> + Sized {
    /// Wrap this handler with the given layer.
    fn with<L>(self, layer: L) -> L::Handler
    where
        L: Layer<Self>,
    {
        layer.layer(self)
    }
}

impl<M, C, S, H> HandlerExt<M, C, S> for H where H: Handler<M, C, S> {}

/// A layer that wraps every handler of a router whose deliveries stay on the thread that polls
/// them: the [`BlanketLayer`] of the local family.
///
/// A runner that hosts the app on one thread (a device's embassy executor) runs every handler
/// through [`LocalHandler`], so a router-scope middleware there wraps a [`LocalHandler`] and
/// hands one back; neither the wrapped handler nor its future needs to be `Send`.
/// [`Identity`], [`Stack`] and [`TracingLayer`](layers::TracingLayer) are layers of both families.
///
/// # Examples
///
/// ```
/// use core::sync::atomic::{AtomicU32, Ordering};
/// use std::convert::Infallible;
///
/// use ruststream::runtime::{Context, HandlerOutcome, LocalHandle, Router, local_subscriber};
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{
///     Deserialized, Input, LocalBlanketLayer, LocalHandler, SoloDeserialized,
/// };
///
/// /// Counts the deliveries that reach the router's handlers.
/// struct CountDeliveries(&'static AtomicU32);
///
/// struct Counted<H>(H, &'static AtomicU32);
///
/// impl LocalBlanketLayer for CountDeliveries {
///     fn apply<M, C, S, H>(&self, handler: H) -> impl LocalHandler<M, C, S>
///     where
///         H: LocalHandler<M, C, S>,
///     {
///         Counted(handler, self.0)
///     }
/// }
///
/// impl<M, C, S, H: LocalHandler<M, C, S>> LocalHandler<M, C, S> for Counted<H> {
///     fn handle(
///         &self,
///         msg: &M,
///         ctx: &mut Context<'_, C, S>,
///     ) -> impl Future<Output = HandlerOutcome> {
///         self.1.fetch_add(1, Ordering::Relaxed);
///         self.0.handle(msg, ctx)
///     }
/// }
///
/// /// A reading as the device's sensor bus frames it, viewed in place.
/// struct Reading<'a>(&'a [u8]);
///
/// impl Deserialized for Reading<'_> {
///     type Output<'a> = Reading<'a>;
///     type Error = Infallible;
///
///     fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> {
///         Ok(Reading(payload))
///     }
/// }
///
/// impl Input for Reading<'_> {
///     type Axis = SoloDeserialized<Reading<'static>>;
/// }
///
/// struct Log;
///
/// impl LocalHandle<Reading<'_>> for Log {
///     async fn handle(
///         &self,
///         reading: &Reading<'_>,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         let _ = reading.0.len();
///         Ok(())
///     }
/// }
///
/// static REACHED: AtomicU32 = AtomicU32::new(0);
///
/// let routes = Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
///     .layer(CountDeliveries(&REACHED))
///     .include(local_subscriber(LocalMemorySource::new("readings"), Log).build());
/// # let _ = routes;
/// ```
///
/// The metrics and OpenTelemetry layers wrap the multi-thread runner's handlers only, so a local
/// router does not take one:
///
/// ```compile_fail
/// # use std::convert::Infallible;
/// # use std::future::ready;
/// use ruststream::metrics::Metrics;
/// use ruststream::runtime::{Context, HandlerOutcome, LocalHandle, Router, local_subscriber};
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{Deserialized, Input, OpenRoutes, SoloDeserialized};
/// # struct Reading<'a>(&'a [u8]);
/// # impl Deserialized for Reading<'_> {
/// #     type Output<'a> = Reading<'a>;
/// #     type Error = Infallible;
/// #     fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> { Ok(Reading(payload)) }
/// # }
/// # impl Input for Reading<'_> { type Axis = SoloDeserialized<Reading<'static>>; }
/// # struct Log;
/// # impl LocalHandle<Reading<'_>> for Log {
/// #     fn handle(&self, r: &Reading<'_>, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { let _ = r.0; ready(Ok(())) }
/// # }
/// fn routes(metrics: &Metrics) -> impl OpenRoutes<LocalMemoryBroker<4>> {
///     Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
///         .layer(metrics.consume_layer())
///         .include(local_subscriber(LocalMemorySource::new("readings"), Log).build())
/// }
/// ```
///
/// ```compile_fail
/// # use std::convert::Infallible;
/// # use std::future::ready;
/// use ruststream::otel::OpenTelemetryLayer;
/// use ruststream::runtime::{Context, HandlerOutcome, LocalHandle, Router, local_subscriber};
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{Deserialized, Input, OpenRoutes, SoloDeserialized};
/// # struct Reading<'a>(&'a [u8]);
/// # impl Deserialized for Reading<'_> {
/// #     type Output<'a> = Reading<'a>;
/// #     type Error = Infallible;
/// #     fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> { Ok(Reading(payload)) }
/// # }
/// # impl Input for Reading<'_> { type Axis = SoloDeserialized<Reading<'static>>; }
/// # struct Log;
/// # impl LocalHandle<Reading<'_>> for Log {
/// #     fn handle(&self, r: &Reading<'_>, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { let _ = r.0; ready(Ok(())) }
/// # }
/// fn routes() -> impl OpenRoutes<LocalMemoryBroker<4>> {
///     Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
///         .layer(OpenTelemetryLayer)
///         .include(local_subscriber(LocalMemorySource::new("readings"), Log).build())
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not wrap the handlers of a local router",
    note = "a router started with `Router::local()` wraps its handlers through \
            `LocalBlanketLayer`, which wraps a `LocalHandler`; a `BlanketLayer` wraps the handlers \
            of the multi-thread runner"
)]
pub trait LocalBlanketLayer {
    /// Wraps `handler`, handing back the handler the router runs in its place.
    fn apply<M, C, S, H>(&self, handler: H) -> impl LocalHandler<M, C, S>
    where
        H: LocalHandler<M, C, S>;
}

/// The identity [`Layer`]: returns the handler unchanged. The default global stack on
/// [`RustStream`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html).
#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl<H> Layer<H> for Identity {
    type Handler = H;

    fn layer(&self, inner: H) -> H {
        inner
    }
}

impl LocalBlanketLayer for Identity {
    fn apply<M, C, S, H>(&self, handler: H) -> impl LocalHandler<M, C, S>
    where
        H: LocalHandler<M, C, S>,
    {
        handler
    }
}

impl BlanketLayer for Identity {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        handler
    }
}

/// Composes two layers into one: `inner` is applied first (innermost), `outer` wraps it.
///
/// Built by chaining [`RustStream::layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.layer); you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct Stack<Inner, Outer> {
    inner: Inner,
    outer: Outer,
}

impl<Inner, Outer> Stack<Inner, Outer> {
    /// Composes `inner` (applied first) under `outer`.
    #[must_use]
    pub fn new(inner: Inner, outer: Outer) -> Self {
        Self { inner, outer }
    }
}

impl<H, Inner, Outer> Layer<H> for Stack<Inner, Outer>
where
    Inner: Layer<H>,
    Outer: Layer<Inner::Handler>,
{
    type Handler = Outer::Handler;

    fn layer(&self, inner: H) -> Self::Handler {
        self.outer.layer(self.inner.layer(inner))
    }
}

impl<Inner, Outer> LocalBlanketLayer for Stack<Inner, Outer>
where
    Inner: LocalBlanketLayer,
    Outer: LocalBlanketLayer,
{
    fn apply<M, C, S, H>(&self, handler: H) -> impl LocalHandler<M, C, S>
    where
        H: LocalHandler<M, C, S>,
    {
        // The order of the `BlanketLayer` impl: inner wraps first, outer outside.
        LocalBlanketLayer::apply::<M, C, S, _>(
            &self.outer,
            LocalBlanketLayer::apply::<M, C, S, _>(&self.inner, handler),
        )
    }
}

impl<Inner, Outer> BlanketLayer for Stack<Inner, Outer>
where
    Inner: BlanketLayer,
    Outer: BlanketLayer,
{
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        // Same order as the static `Layer` impl: inner wraps first (innermost), outer outside.
        self.outer
            .apply::<M, C, S, _>(self.inner.apply::<M, C, S, _>(handler))
    }
}

/// Bundled, opinionated middleware layers ready to drop into a handler stack.
pub mod layers {
    use tracing::{debug, info, instrument, warn};

    use super::super::handler::HandlerResult;
    use super::{
        BlanketLayer, Context, Handler, HandlerOutcome, Layer, LocalBlanketLayer, LocalHandler,
    };

    /// Logs every delivery and its outcome via [`tracing`]. Default level is `INFO` for the
    /// outcome and `DEBUG` for arrival.
    #[derive(Debug, Clone, Default)]
    pub struct TracingLayer {
        target: Option<&'static str>,
    }

    impl TracingLayer {
        /// Constructs a layer that emits events under the given tracing target.
        #[must_use]
        pub const fn with_target(target: &'static str) -> Self {
            Self {
                target: Some(target),
            }
        }
    }

    impl<H> Layer<H> for TracingLayer {
        type Handler = TracingHandler<H>;

        fn layer(&self, inner: H) -> Self::Handler {
            TracingHandler {
                inner,
                target: self.target,
            }
        }
    }

    impl BlanketLayer for TracingLayer {
        fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
        where
            M: Send + Sync + 'static,
            C: Send + 'static,
            S: Send + Sync + 'static,
            H: Handler<M, C, S> + 'static,
        {
            self.layer(handler)
        }
    }

    impl LocalBlanketLayer for TracingLayer {
        fn apply<M, C, S, H>(&self, handler: H) -> impl LocalHandler<M, C, S>
        where
            H: LocalHandler<M, C, S>,
        {
            self.layer(handler)
        }
    }

    /// Handler produced by [`TracingLayer::layer`].
    #[derive(Debug, Clone)]
    pub struct TracingHandler<H> {
        inner: H,
        target: Option<&'static str>,
    }

    impl<M, C, S, H> Handler<M, C, S> for TracingHandler<H>
    where
        M: Sync,
        C: Send,
        S: Send + Sync,
        H: Handler<M, C, S>,
    {
        #[instrument(level = "trace", skip(self, msg, ctx), fields(target = self.target))]
        fn handle(
            &self,
            msg: &M,
            ctx: &mut Context<'_, C, S>,
        ) -> impl Future<Output = HandlerOutcome> + Send {
            // An `async move` block, so `instrument` enters its span around the whole future.
            async move { traced(move || self.inner.handle(msg, ctx)).await }
        }
    }

    impl<M, C, S, H> LocalHandler<M, C, S> for TracingHandler<H>
    where
        H: LocalHandler<M, C, S>,
    {
        // The delivery stays on the thread that polls it, which is what this twin is for.
        #[allow(clippy::future_not_send)]
        #[instrument(level = "trace", skip(self, msg, ctx), fields(target = self.target))]
        fn handle(
            &self,
            msg: &M,
            ctx: &mut Context<'_, C, S>,
        ) -> impl Future<Output = HandlerOutcome> {
            // An `async move` block, so `instrument` enters its span around the whole future.
            async move { traced(move || self.inner.handle(msg, ctx)).await }
        }
    }

    /// Logs the delivery, runs the wrapped handler and logs its outcome: the body of both
    /// families' [`TracingHandler`].
    // `Send` exactly where the wrapped handler's future is: the `Handler` impl proves it there.
    #[allow(clippy::future_not_send)]
    async fn traced<Handled>(handle: impl FnOnce() -> Handled) -> HandlerOutcome
    where
        Handled: Future<Output = HandlerOutcome>,
    {
        debug!(target: "ruststream::dispatch", "delivery received");
        // Log the outcome inside the settlement; the continuation (if any) flows through.
        let settle = handle().await;
        log_outcome(&settle);
        settle
    }

    /// Logs what the wrapped handler decided.
    fn log_outcome(settle: &HandlerOutcome) {
        match settle.outcome() {
            HandlerResult::Ack => {
                info!(target: "ruststream::dispatch", "handler ack");
            }
            HandlerResult::Nack { requeue } => {
                warn!(target: "ruststream::dispatch", requeue, "handler nack");
            }
            HandlerResult::NackAfter { delay } => {
                warn!(target: "ruststream::dispatch", ?delay, "handler delayed nack");
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use core::time::Duration;

        use super::*;
        use crate::HeaderMap;
        use crate::dispatch::Delivery;

        struct Fixed(HandlerResult);

        impl Handler<(), (), ()> for Fixed {
            fn handle(
                &self,
                _msg: &(),
                _ctx: &mut Context<'_, (), ()>,
            ) -> impl Future<Output = HandlerOutcome> + Send {
                let outcome = self.0;
                async move { HandlerOutcome::from(outcome) }
            }
        }

        async fn traced(layer: &TracingLayer, outcome: HandlerResult) -> HandlerResult {
            let state = ();
            let delivery = Delivery::empty();
            let headers = HeaderMap::new();
            let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
            layer
                .layer(Fixed(outcome))
                .handle(&(), &mut ctx)
                .await
                .outcome()
        }

        #[tokio::test]
        async fn the_tracing_layer_logs_every_outcome_and_passes_it_through() {
            // Logging must never rewrite the settlement, whichever branch reports it.
            let layer = TracingLayer::default();
            assert_eq!(traced(&layer, HandlerResult::Ack).await, HandlerResult::Ack);
            assert_eq!(
                traced(&layer, HandlerResult::retry()).await,
                HandlerResult::retry()
            );
            let delayed = HandlerResult::retry_after(Duration::from_secs(3));
            assert_eq!(traced(&layer, delayed).await, delayed);
        }

        #[tokio::test]
        async fn a_target_scoped_layer_wraps_the_same_way() {
            let layer = TracingLayer::with_target("my_service::orders");
            assert_eq!(traced(&layer, HandlerResult::Ack).await, HandlerResult::Ack);
        }
    }
}
