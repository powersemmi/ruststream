//! The publish pipeline: the static layer chain and its opt-in dynamic counterpart.

use std::{fmt, future::Future, sync::Arc};

use crate::runtime::lifecycle::BoxError;
// `DefaultCodec` only exists when a codec feature is on; the impl that names it is gated the same
// way, so an ungated import would break `--no-default-features`.
use super::{Outgoing, PublishFut};
use crate::{OutgoingMessage, Publisher};

/// A static, app-wide publish pipeline: an around-style chain of [`PublishLayer`] ending in
/// the broker send.
///
/// The publish-side analog of the consume-side static [`Stack`](super::Stack) /
/// [`Identity`](super::Identity): the
/// app's publish middleware (added with
/// [`RustStream::publish_layer`](super::RustStream::publish_layer)) compose into a concrete type, so
/// the default path ([`PublishIdentity`], no middleware) is a zero-cost direct send with no `dyn`
/// dispatch. You rarely name this trait; it is built for you. (A runtime-composed escape hatch, the
/// publish counterpart of [`DynStack`](super::DynStack), can be layered in later without changing
/// this contract.)
pub trait PublishPipeline: Send + Sync {
    /// Runs `out` through the remaining middleware, then sends it via `send`.
    fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a;
}

/// The terminal [`PublishPipeline`]: no middleware, just the broker send. The default for an app
/// with no [`publish_layer`](super::RustStream::publish_layer).
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishIdentity;

impl PublishPipeline for PublishIdentity {
    async fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
    ) -> Result<(), BoxError> {
        let msg =
            OutgoingMessage::new(out.name(), out.payload()).with_headers(out.headers().clone());
        send.publish(msg).await.map_err(|e| Box::new(e) as BoxError)
    }
}

/// Prepends a [`PublishLayer`] `Head` to a [`PublishPipeline`] `Tail`. Built by
/// [`RustStream::publish_layer`](super::RustStream::publish_layer); you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishStack<Head, Tail> {
    head: Head,
    tail: Tail,
}

impl<Head, Tail> PublishStack<Head, Tail> {
    /// Composes `head` in front of `tail`.
    pub(crate) const fn new(head: Head, tail: Tail) -> Self {
        Self { head, tail }
    }
}

impl<Head: PublishLayer, Tail: PublishPipeline> PublishPipeline for PublishStack<Head, Tail> {
    fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        self.head.on_publish(
            out,
            PublishNext {
                tail: &self.tail,
                send,
            },
        )
    }
}

/// Middleware that transforms (or observes) an [`Outgoing`] message before it is published.
///
/// Each middleware inspects / mutates `out`, then calls [`PublishNext::run`] to continue; the chain
/// ends in the actual broker publish. Static (no `dyn` dispatch): a middleware is generic over the
/// rest of the pipeline `N`, so the whole chain monomorphizes. Added app-wide with
/// [`RustStream::publish_layer`](super::RustStream::publish_layer).
pub trait PublishLayer: Send + Sync {
    /// Handle the outgoing message, calling `next` to continue the pipeline.
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a;
}

/// A cursor over the rest of the publish pipeline, ending in the broker send. Handed to a
/// [`PublishLayer`]; call [`run`](Self::run) to continue.
pub struct PublishNext<'a, N, P> {
    tail: &'a N,
    send: &'a P,
}

impl<'a, N: PublishPipeline, P: Publisher> PublishNext<'a, N, P> {
    /// Runs the rest of the pipeline (the remaining middleware, then the send).
    pub fn run(
        self,
        out: &'a mut Outgoing<'a>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        self.tail.run(out, self.send)
    }
}

impl<N, P> fmt::Debug for PublishNext<'_, N, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishNext").finish_non_exhaustive()
    }
}

/// An object-safe publish middleware, for a [`PublishDynStack`].
///
/// The dynamic counterpart of [`PublishLayer`]: it cannot name the rest of the pipeline as a
/// type parameter (that is what keeps it object-safe and lets a heterogeneous, runtime-built list
/// live in one [`PublishDynStack`]), so it continues through the type-erased [`PublishDynNext`]
/// instead. Use it only when the middleware set is decided at runtime; otherwise a static
/// [`PublishLayer`] is zero-cost.
pub trait PublishDynLayer: Send + Sync {
    /// Handle the outgoing message, calling `next` to continue.
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishDynNext<'a>,
    ) -> PublishFut<'a>;
}

/// A cursor over the rest of a [`PublishDynStack`], ending in the surrounding static pipeline.
///
/// Mirrors [`PublishNext`] for the dynamic list: [`run`](Self::run) advances to the next
/// [`PublishDynLayer`], or hands control back to the static chain once the list is exhausted.
pub struct PublishDynNext<'a> {
    rest: &'a [Arc<dyn PublishDynLayer>],
    // The surrounding static `PublishNext::run`, erased so this cursor need not carry its type. A
    // one-shot continuation: `run` is called exactly once per published message.
    tail: Box<dyn FnOnce(&'a mut Outgoing<'a>) -> PublishFut<'a> + Send + 'a>,
}

impl<'a> PublishDynNext<'a> {
    /// Runs the next dynamic middleware, or the surrounding static pipeline if the list is done.
    #[must_use]
    pub fn run(self, out: &'a mut Outgoing<'a>) -> PublishFut<'a> {
        match self.rest.split_first() {
            Some((middleware, rest)) => middleware.on_publish(
                out,
                PublishDynNext {
                    rest,
                    tail: self.tail,
                },
            ),
            None => (self.tail)(out),
        }
    }
}

impl fmt::Debug for PublishDynNext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishDynNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// A single static [`PublishLayer`] wrapping a runtime-built, frozen list of
/// [`PublishDynLayer`].
///
/// The publish-side counterpart of the consume-side [`DynStack`](super::DynStack): the opt-in
/// escape hatch for a middleware set decided at runtime (from config, a loop, feature flags) that
/// therefore cannot be a compile-time [`publish_layer`](super::RustStream::publish_layer) chain.
/// Add it like any other middleware; only the middleware inside it pay one boxed future per layer.
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # {
/// use std::sync::Arc;
/// use ruststream::runtime::{PublishDynLayer, PublishDynStack};
///
/// fn stack(
///     middleware: Vec<Arc<dyn PublishDynLayer>>,
/// ) -> PublishDynStack {
///     PublishDynStack::new(middleware)
/// }
/// # }
/// ```
pub struct PublishDynStack(Arc<[Arc<dyn PublishDynLayer>]>);

// Manual `Clone`: the field is an `Arc`, so a clone is a refcount bump regardless of the contents.
impl Clone for PublishDynStack {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl PublishDynStack {
    /// Builds a stack from a list of middleware, applied in iteration order (first runs outermost).
    #[must_use]
    pub fn new(middleware: impl IntoIterator<Item = Arc<dyn PublishDynLayer>>) -> Self {
        Self(middleware.into_iter().collect())
    }
}

impl fmt::Debug for PublishDynStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishDynStack")
            .field("middleware", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl PublishLayer for PublishDynStack {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        // Erase the static continuation into a one-shot closure so the object-safe walker can end
        // by handing control back to the surrounding static pipeline. Boxing starts here: only
        // the dynamic list pays it.
        PublishDynNext {
            rest: &self.0,
            tail: Box::new(move |out| Box::pin(next.run(out)) as PublishFut<'a>),
        }
        .run(out)
    }
}
