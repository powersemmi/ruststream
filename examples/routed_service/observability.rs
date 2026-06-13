//! Cross-cutting observability: an application-scope middleware that logs and times every
//! delivery.
//!
//! It implements both [`Layer`] (to wrap handlers mounted directly on a scope) and
//! [`BlanketLayer`] - the latter is what lets `RustStream::layer` reach handlers mounted through a
//! router, whose concrete types the router hides behind one generic method. The metrics consume
//! layer is applied per router instead (in [`routes`](crate::routes)) to keep that metric scoped to
//! the routed handlers and to show off [`Router::layer`](ruststream::runtime::Router::layer); this
//! observability layer is global, so it carries the whole stack and must be a `BlanketLayer`.

use std::time::Instant;

use ruststream::runtime::{BlanketLayer, Context, Handler, HandlerResult, Layer};

/// The layer value added with `RustStream::layer`.
#[derive(Clone)]
pub(crate) struct Observe;

/// The handler `Observe` wraps around an inner handler.
pub(crate) struct Observed<H>(H);

impl<H> Layer<H> for Observe {
    type Handler = Observed<H>;
    fn layer(&self, inner: H) -> Observed<H> {
        Observed(inner)
    }
}

impl BlanketLayer for Observe {
    fn apply<M, H>(&self, handler: H) -> impl Handler<M> + 'static
    where
        M: Send + Sync + 'static,
        H: Handler<M> + 'static,
    {
        Observed(handler)
    }
}

impl<M: Send + Sync, H: Handler<M>> Handler<M> for Observed<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        let channel = ctx.name().to_owned();
        let started = Instant::now();
        let result = self.0.handle(msg, ctx).await;
        tracing::info!(channel = %channel, elapsed = ?started.elapsed(), "handled");
        result
    }
}
