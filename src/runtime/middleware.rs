//! Middleware infrastructure: [`Layer`] composes wrappers around handlers, tower-style.
//!
//! A `Layer` consumes one handler and returns another. Stacking layers builds the final
//! handler the router invokes. Layers run in the order they are applied: the outermost
//! `with(...)` runs first.
//!
//! # Examples
//!
//! ```
//! use ruststream::IncomingMessage;
//! use ruststream::runtime::{Context, Handler, HandlerExt, HandlerOutcome, layers::TracingLayer};
//!
//! fn build<M: IncomingMessage + 'static>() -> impl Handler<M> {
//!     let base = |_msg: &M, _ctx: &mut Context| async { HandlerOutcome::ack() };
//!     base.with(TracingLayer::default())
//! }
//! ```

use std::future::Future;

use super::context::Context;
use super::handler::{Handler, HandlerOutcome};

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
/// through [`include_router`](super::BrokerScope::include_router): its handlers are erased behind
/// [`RouterDef`](super::RouterDef). `BlanketLayer` carries the wrapping as a generic method, so a
/// layer that applies uniformly (logging, metrics) can wrap every router handler from one bound.
///
/// Implemented for [`Identity`], a [`Stack`] of blanket layers, and the bundled
/// [`TracingLayer`](layers::TracingLayer). Implement it for a custom layer to let the app's global
/// stack reach router handlers; a layer that only wraps specific handler types cannot be blanket.
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

/// The identity [`Layer`]: returns the handler unchanged. The default global stack on
/// [`RustStream`](super::RustStream).
#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl<H> Layer<H> for Identity {
    type Handler = H;

    fn layer(&self, inner: H) -> H {
        inner
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
/// Built by chaining [`RustStream::layer`](super::RustStream::layer); you rarely name it directly.
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
    use super::{BlanketLayer, Context, Future, Handler, HandlerOutcome, Layer};

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
            async move {
                debug!(target: "ruststream::dispatch", "delivery received");
                // Log the outcome inside the settlement; the continuation (if any) flows through.
                let settle = self.inner.handle(msg, ctx).await;
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
                settle
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::time::Duration;

        use super::*;
        use crate::HeaderMap;
        use crate::runtime::dispatch::Delivery;

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
