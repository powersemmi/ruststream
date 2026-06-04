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
//! use ruststream::runtime::{Context, Handler, HandlerExt, HandlerResult, layers::TracingLayer};
//!
//! fn build<M: IncomingMessage + 'static>() -> impl Handler<M> {
//!     let base = |_msg: &M, _ctx: &mut Context| async { HandlerResult::Ack };
//!     base.with(TracingLayer::default())
//! }
//! ```

use std::future::Future;

use super::context::Context;
use super::handler::{Handler, HandlerResult};

/// A function from one handler to another. Apply with [`HandlerExt::with`].
pub trait Layer<H> {
    /// The handler type produced by this layer.
    type Handler;

    /// Wrap `inner` and return the composed handler.
    fn layer(&self, inner: H) -> Self::Handler;
}

/// Convenience extension trait for fluent layer stacking on any [`Handler`].
pub trait HandlerExt<M>: Handler<M> + Sized {
    /// Wrap this handler with the given layer.
    fn with<L>(self, layer: L) -> L::Handler
    where
        L: Layer<Self>,
    {
        layer.layer(self)
    }
}

impl<M, H> HandlerExt<M> for H where H: Handler<M> {}

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

/// Bundled, opinionated middleware layers ready to drop into a handler stack.
pub mod layers {
    use tracing::{debug, info, instrument, warn};

    use super::{Context, Future, Handler, HandlerResult, Layer};

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

    /// Handler produced by [`TracingLayer::layer`].
    #[derive(Debug, Clone)]
    pub struct TracingHandler<H> {
        inner: H,
        target: Option<&'static str>,
    }

    impl<M, H> Handler<M> for TracingHandler<H>
    where
        M: Sync,
        H: Handler<M>,
    {
        #[instrument(level = "trace", skip(self, msg, ctx), fields(target = self.target))]
        fn handle(&self, msg: &M, ctx: &mut Context) -> impl Future<Output = HandlerResult> + Send {
            async move {
                debug!(target: "ruststream::dispatch", "delivery received");
                let result = self.inner.handle(msg, ctx).await;
                match result {
                    HandlerResult::Ack => {
                        info!(target: "ruststream::dispatch", "handler ack");
                    }
                    HandlerResult::Nack { requeue } => {
                        warn!(target: "ruststream::dispatch", requeue, "handler nack");
                    }
                }
                result
            }
        }
    }
}
