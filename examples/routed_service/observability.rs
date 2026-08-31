//! Cross-cutting observability: a consume-side middleware ([`Observe`]) that logs and times every
//! delivery, and a publish-side layer ([`StampSource`]) that stamps a provenance header on every
//! reply.
//!
//! It implements both [`Layer`] (to wrap handlers mounted directly on a scope) and
//! [`BlanketLayer`] - the latter is what lets `RustStream::layer` reach handlers mounted through a
//! router, whose concrete types the router hides behind one generic method. The metrics consume
//! layer is applied per router instead (in [`routes`](crate::routes)) to keep that metric scoped to
//! the routed handlers and to show off [`Router::layer`](ruststream::runtime::Router::layer); this
//! observability layer is global, so it carries the whole stack and must be a `BlanketLayer`.

use std::time::Instant;

use ruststream::runtime::{
    BlanketLayer, Context, Handler, HandlerOutcome, Layer, Outgoing, PublishTransform,
};

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
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Observed(handler)
    }
}

impl<M: Send + Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S>
    for Observed<H>
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        let channel = ctx.name().to_owned();
        let started = Instant::now();
        let outcome = self.0.handle(msg, ctx).await;
        tracing::info!(channel = %channel, elapsed = ?started.elapsed(), "handled");
        outcome
    }
}

/// A static publish-side layer: stamps a provenance header on every message a publisher sends.
///
/// This is the other kind of publisher customisation. Where the consume `Observe` layer wraps
/// handlers, a [`PublishTransform`] is composed onto one [`TypedPublisher`](ruststream::runtime::TypedPublisher)
/// at build time with `.layer(..)` - zero-cost and scoped to that publisher, unlike the dynamic,
/// app-wide metrics middleware added with `RustStream::publish_layer`. [`routes`](crate::routes)
/// attaches it to the confirmations publisher, so every confirmation carries the header.
pub(crate) struct StampSource;

impl<C> PublishTransform<C> for StampSource {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &ruststream::runtime::PublishContext<'_, C>) {
        out.headers_mut()
            .insert("x-source-service", b"orders-service".to_vec());
    }
}
