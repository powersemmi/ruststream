//! The publish path of one [`Out`](crate::runtime::Out) slot: the mount site's per-slot transform
//! stack, then the app-wide publish pipeline, then the slot's attributed leaf.
//!
//! A slot publish is issued by the handler body itself, so it never passes the dispatch that
//! carries a reply. The pieces here are what puts the same wiring on it anyway: the mount site's
//! `.out(marker, policy).transform(..)` steps compose into an [`OutTransformStack`], the stack
//! lowers into the app's own [`PublishPipeline`] as its outermost layer
//! ([`LowerOutTransforms`]), and the entry sends through the composed pipeline
//! ([`OutPipeline`]). A slot that names no transform in an app that adds no
//! [`publish_layer`](crate::runtime::RustStream::publish_layer) keeps
//! [`PublishIdentity`] there, which sends the message straight to the leaf - the same call the
//! entry made before any of this existed.

use std::error::Error as StdError;
use std::future::Future;

use bytes::BytesMut;
use thiserror::Error;

use super::{Outgoing, PublishIdentity, PublishLayer, PublishNext, PublishPipeline, PublishStack};
use crate::runtime::lifecycle::BoxError;
use crate::{OutgoingMessage, Publisher};

/// A static transform on every message leaving one [`Out`](crate::runtime::Out) slot: it mutates
/// the encoded [`Outgoing`] before the app-wide publish pipeline and the broker send.
///
/// The slot counterpart of [`PublishTransform`](super::PublishTransform), composed onto a slot by
/// the `.out(marker, policy).transform(..)` step of a mount site's chain. Use it for what belongs
/// to the destination rather than to the message: an outbox envelope, a fixed content-type header,
/// a tenant tag.
///
/// It takes no [`PublishContext`](super::PublishContext), because a slot publish has none to read:
/// the handler body issues it, so the delivery that prompted it is the body's own to read from its
/// [`Context`](crate::runtime::Context) and put on the message. A transform wanted on both a reply
/// and a slot implements both traits; the two bodies are the same line.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{HandlerOutcome, Out, Outgoing, OutTransform};
/// # use ruststream::{OutSlot, Outgoing as OutgoingDerive, subscriber};
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
/// # #[derive(OutgoingDerive, serde::Serialize, schemars::JsonSchema)]
/// # #[outgoing(name = "audit.orders")]
/// # struct Audited { id: u64 }
/// # #[derive(OutSlot)]
/// # #[publishes(Audited)]
/// # struct Audit;
/// # #[subscriber("orders")]
/// # async fn mirror(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
/// #     if audit.message(&Audited { id: order.id }).publish().await.is_err() {
/// #         return HandlerOutcome::retry();
/// #     }
/// #     HandlerOutcome::ack()
/// # }
///
/// struct Envelope;
///
/// impl OutTransform for Envelope {
///     fn apply(&self, out: &mut Outgoing<'_>) {
///         out.headers_mut().insert("x-outbox", b"1".to_vec());
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(mirror).out(Audit, Publish).transform(Envelope).build();
///     })
/// }
/// # }
/// ```
pub trait OutTransform: Send + Sync {
    /// Transforms `out` in place before the app-wide pipeline runs and the message is sent.
    fn apply(&self, out: &mut Outgoing<'_>);
}

/// The empty [`OutTransform`] stack: what a slot carries until a `.transform(..)` step composes
/// one onto it.
///
/// It never reaches a publish: a slot whose stack is still empty lowers into the app's pipeline
/// unchanged (see [`LowerOutTransforms`]), so nothing runs and nothing is paid.
#[derive(Debug, Clone, Copy, Default)]
pub struct OutTransformIdentity;

impl OutTransform for OutTransformIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>) {}
}

/// Composes two [`OutTransform`]s: `inner` runs first, then `outer`. Built by a chain's
/// `.transform(..)` step; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct OutTransformStack<Inner, Outer> {
    // The slot attachment builds the stack when a transform is layered on.
    pub(crate) inner: Inner,
    pub(crate) outer: Outer,
}

impl<Inner: OutTransform, Outer: OutTransform> OutTransform for OutTransformStack<Inner, Outer> {
    fn apply(&self, out: &mut Outgoing<'_>) {
        self.inner.apply(out);
        self.outer.apply(out);
    }
}

// The stack is its own publish layer, which is how it reaches the app-wide pipeline without a
// wrapper: it runs the whole stack, then hands the message to the rest of the chain.
impl<Inner: OutTransform, Outer: OutTransform> PublishLayer for OutTransformStack<Inner, Outer> {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        self.apply(out);
        next.run(out)
    }
}

/// Lowers a slot's [`OutTransform`] stack onto the app's publish pipeline, producing the pipeline
/// the slot entry publishes through. Machinery; never named in user code.
///
/// The empty stack lowers to the app's pipeline unchanged, so a slot that names no transform in an
/// app that adds no middleware keeps [`PublishIdentity`] and publishes with nothing in the way. A
/// non-empty stack becomes the outermost layer, which is the reply path's order too: the mount
/// site's own transforms run first (closest to the encoded value), then the app-wide middleware,
/// then the send.
#[doc(hidden)]
pub trait LowerOutTransforms<Pipeline> {
    /// The slot's composed publish pipeline.
    type Out;

    /// Composes it.
    fn lower(self, pipeline: Pipeline) -> Self::Out;
}

impl<Pipeline> LowerOutTransforms<Pipeline> for OutTransformIdentity {
    type Out = Pipeline;

    fn lower(self, pipeline: Pipeline) -> Pipeline {
        pipeline
    }
}

impl<Pipeline, Inner, Outer> LowerOutTransforms<Pipeline> for OutTransformStack<Inner, Outer> {
    type Out = PublishStack<Self, Pipeline>;

    fn lower(self, pipeline: Pipeline) -> Self::Out {
        PublishStack::new(self, pipeline)
    }
}

/// The composed publish pipeline of one [`Out`](crate::runtime::Out) slot, as the entry sends
/// through it.
///
/// A slot entry carries the pipeline its mount site composed - the app-wide
/// [`publish_layer`](crate::runtime::RustStream::publish_layer) chain with the slot's own
/// transforms on top - and this is how a publish travels it. It is implemented for exactly the two
/// shapes an app's pipeline can have, because those are the two an app can build:
/// [`PublishIdentity`] (nothing to run: the message goes straight to the leaf, with the leaf's own
/// error) and [`PublishStack`] (the chain runs, and its errors travel boxed as a
/// [`PipelinePublishError`], like every other message that goes through publish middleware).
///
/// A hand-written [`Handle`](crate::runtime::Handle) body generic over its slot entry names this
/// bound on the entry's pipeline parameter; nothing else does.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a slot's publish pipeline",
    note = "a slot publishes through the app's own pipeline: `PublishIdentity` when the app adds \
            no `publish_layer`, and the `PublishStack` those calls build otherwise"
)]
pub trait OutPipeline: Send + Sync {
    /// The error a publish through this pipeline reports, over the leaf publisher's own error
    /// `E`: the leaf's error itself when nothing runs, a [`PipelinePublishError`] once middleware
    /// does.
    type Error<E: StdError + Send + Sync + 'static>: StdError + Send + Sync + 'static;

    /// Reports a leaf-publisher error that did not travel this pipeline (a transaction call, a
    /// request round trip) in the entry's error type.
    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> Self::Error<E>;

    /// Sends one message through the pipeline into `leaf`, the slot's attributed publisher.
    fn send<P: Publisher>(
        &self,
        leaf: &P,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error<P::Error>>> + Send;
}

impl OutPipeline for PublishIdentity {
    type Error<E: StdError + Send + Sync + 'static> = E;

    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> E {
        err
    }

    // Nothing composed onto this slot: the publish is the leaf call it always was, with no
    // message rebuilt and no error rewrapped.
    async fn send<P: Publisher>(&self, leaf: &P, msg: OutgoingMessage<'_>) -> Result<(), P::Error> {
        leaf.publish(msg).await
    }
}

impl<Head: PublishLayer, Tail: PublishPipeline> OutPipeline for PublishStack<Head, Tail> {
    type Error<E: StdError + Send + Sync + 'static> = PipelinePublishError;

    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> PipelinePublishError {
        PipelinePublishError(Box::new(err))
    }

    async fn send<P: Publisher>(
        &self,
        leaf: &P,
        msg: OutgoingMessage<'_>,
    ) -> Result<(), PipelinePublishError> {
        // The pipeline mutates the message, so the borrowed publish takes ownership of its parts
        // here; only a slot that actually has middleware pays for that.
        let mut out = Outgoing::new(msg.name(), BytesMut::from(msg.payload()));
        *out.headers_mut() = msg.headers().clone();
        self.run(&mut out, leaf).await.map_err(PipelinePublishError)
    }
}

/// The error of a publish that travelled a publish pipeline: a middleware rejected the message, or
/// the broker did.
///
/// The pipeline is generic over the publisher it ends in, so the broker's own error type does not
/// survive the chain; it travels as this error's [`source`](std::error::Error::source), the way a
/// reply's does.
#[derive(Debug, Error)]
#[error("failed to publish through the publish pipeline")]
pub struct PipelinePublishError(#[source] BoxError);

impl PipelinePublishError {
    /// The error the middleware chain or the broker reported.
    #[must_use]
    pub fn into_source(self) -> BoxError {
        self.0
    }
}
