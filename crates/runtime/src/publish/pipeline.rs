//! The publish pipeline: the static layer chain and its opt-in dynamic counterpart.

use core::fmt;
use ruststream_core::{MaybeSend, MaybeSendSync, MaybeSync};

use super::Outgoing;
use crate::lifecycle::BoxError;
use crate::publish_failure::Returned;
#[cfg(feature = "testing")]
use crate::testing::coordinator::PipelinePublish;
use crate::{PayloadForm, Publisher};

/// A static, app-wide publish pipeline: an around-style chain of [`PublishLayer`] ending in
/// the broker send.
///
/// The publish-side analog of the consume-side static [`Stack`](crate::Stack) /
/// [`Identity`](crate::Identity): the
/// app's publish middleware (added with
/// [`RustStream::publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer)) compose into a concrete type, so
/// the default path ([`PublishIdentity`], no middleware) is a zero-cost direct send with no `dyn`
/// dispatch. You rarely name this trait; it is built for you. (A runtime-composed escape hatch, the
/// publish counterpart of [`DynStack`](crate::DynStack), can be layered in later without changing
/// this contract.)
pub trait PublishPipeline: MaybeSendSync {
    /// Runs `out` through the remaining middleware, then sends it via `send` with the broker's
    /// per-message settings the call site adjusted (`None` on a reply, which has no call site).
    fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        send: &'a P,
        options: Option<&'a P::Options>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a;
}

/// The last step of a typed publish: how the message reaches the leaf publisher `P`.
///
/// Every [`PublishPipeline`] is one for every [`Publisher`], through a forwarding impl whose
/// future is the pipeline's own, so the multi-thread runner's publish stays `Send`. The pipeline
/// of a [`Router::local`](crate::Router::local) chain is one for every
/// [`Publisher`](crate::Publisher). Machinery; never named directly.
#[doc(hidden)]
pub(crate) trait SendThrough<P: Publisher>: MaybeSync {
    /// Sends `out` to `send` with the per-message settings the call site adjusted.
    fn send_through<'a>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        send: &'a P,
        options: Option<&'a P::Options>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a;
}

impl<Pipeline: PublishPipeline, P: Publisher> SendThrough<P> for Pipeline {
    #[inline]
    fn send_through<'a>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        send: &'a P,
        options: Option<&'a P::Options>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a {
        self.run(out, send, options)
    }
}

/// The terminal [`PublishPipeline`]: no middleware, just the broker send. The default for an app
/// with no [`publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer).
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishIdentity;

impl PublishPipeline for PublishIdentity {
    async fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        send: &'a P,
        options: Option<&'a P::Options>,
    ) -> Result<(), BoxError> {
        // The broker is the last reader of this message, so what the stages produced travels
        // into it in the form the publisher declared, and the map they filled moves with it.
        let msg = <P::Payload as PayloadForm>::leaving(&mut *out);
        #[cfg(feature = "testing")]
        let recorded = PipelinePublish::capture(&msg, options);
        // A refusal puts the message back into `out`, so the caller reads what was sent.
        let result = send
            .publish(msg, options)
            .await
            .map_err(Returned::of::<P::Payload>)
            .map_err(|returned| returned.restore::<P::Payload>(out));
        #[cfg(feature = "testing")]
        if result.is_ok() {
            recorded.sent().await;
        }
        result
    }
}

/// Prepends a [`PublishLayer`] `Head` to a [`PublishPipeline`] `Tail`. Built by
/// [`RustStream::publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer); you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishStack<Head, Tail> {
    head: Head,
    tail: Tail,
}

impl<Head, Tail> PublishStack<Head, Tail> {
    /// Composes `head` in front of `tail`.
    #[doc(hidden)]
    pub const fn new(head: Head, tail: Tail) -> Self {
        Self { head, tail }
    }
}

impl<Head: PublishLayer, Tail: PublishPipeline> PublishPipeline for PublishStack<Head, Tail> {
    fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        send: &'a P,
        options: Option<&'a P::Options>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a {
        self.head.on_publish(
            out,
            PublishNext {
                tail: &self.tail,
                send,
                options,
            },
        )
    }
}

/// Middleware that transforms (or observes) an [`Outgoing`] message before it is published.
///
/// Each middleware inspects / mutates `out`, then calls [`PublishNext::run`] to continue; the chain
/// ends in the actual broker publish. Static (no `dyn` dispatch): a middleware is generic over the
/// rest of the pipeline `N`, so the whole chain monomorphizes. Added app-wide with
/// [`RustStream::publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer).
pub trait PublishLayer: MaybeSendSync {
    /// Handle the outgoing message, calling `next` to continue the pipeline.
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a;
}

/// A cursor over the rest of the publish pipeline, ending in the broker send. Handed to a
/// [`PublishLayer`]; call [`run`](Self::run) to continue.
pub struct PublishNext<'a, N, P: Publisher> {
    tail: &'a N,
    send: &'a P,
    options: Option<&'a P::Options>,
}

impl<'a, N: PublishPipeline, P: Publisher> PublishNext<'a, N, P> {
    /// Runs the rest of the pipeline (the remaining middleware, then the send).
    pub fn run(
        self,
        out: &'a mut Outgoing<'_>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a {
        self.tail.run(out, self.send, self.options)
    }
}

impl<N, P: Publisher> fmt::Debug for PublishNext<'_, N, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishNext").finish_non_exhaustive()
    }
}
