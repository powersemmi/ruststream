//! The publish path of one [`Out`](crate::runtime::Out) slot: the mount site's per-slot transform
//! stack, then the app-wide publish pipeline, then the slot's attributed leaf.
//!
//! A slot publish is issued by the handler body itself, so it never passes the dispatch that
//! carries a reply. The pieces here are what puts the same wiring on it anyway: the mount site's
//! `.out(marker, policy).transform(..)` steps compose into the same
//! [`PublishTransformStack`](super::PublishTransformStack) a reply uses, the stack is paired with
//! the slot it was named on ([`SlotTransforms`]) and lowers into the app's own
//! [`PublishPipeline`] as its outermost layer ([`LowerOutTransforms`]), and the entry sends
//! through the composed pipeline ([`OutPipeline`]). A slot that names no transform in an app that
//! adds no
//! [`publish_layer`](crate::runtime::RustStream::publish_layer) keeps
//! [`PublishIdentity`] there, which sends the message straight to the leaf - the same call the
//! entry made before any of this existed.

use std::error::Error as StdError;
use std::future::Future;

use bytes::BytesMut;
use thiserror::Error;

use super::{
    ForSlot, Names, Outgoing, PublishIdentity, PublishLayer, PublishNext, PublishPipeline,
    PublishStack, PublishTransform, PublishTransformIdentity, PublishTransformStack, Reads,
    SlotContext,
};
use crate::runtime::lifecycle::BoxError;
use crate::{ConnectedBroker, HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};

/// One slot's [`PublishTransform`] stack, paired with the slot it was named on so the transforms
/// have their [`SlotContext`] to read. Machinery; a mount site's `.transform(..)` steps build the
/// stack and the wiring pairs it with the marker.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct SlotTransforms<Stack> {
    slot: &'static str,
    stack: Stack,
}

// The pair is its own publish layer, which is how it reaches the app-wide pipeline without a
// wrapper: it runs the whole stack, then hands the message to the rest of the chain.
impl<Stack: PublishTransform<ForSlot>> PublishLayer for SlotTransforms<Stack> {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        self.stack.apply(out, &SlotContext::new(self.slot));
        next.run(out)
    }
}

/// Lowers a slot's [`PublishTransform`] stack onto the app's publish pipeline, producing the
/// pipeline the slot entry publishes through. Machinery; never named in user code.
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

    /// Composes it, against the marker whose name the transforms read.
    fn lower(self, slot: &'static str, pipeline: Pipeline) -> Self::Out;
}

impl<Pipeline> LowerOutTransforms<Pipeline> for PublishTransformIdentity {
    type Out = Pipeline;

    fn lower(self, _slot: &'static str, pipeline: Pipeline) -> Pipeline {
        pipeline
    }
}

impl<Pipeline, Inner, Outer> LowerOutTransforms<Pipeline> for PublishTransformStack<Inner, Outer> {
    type Out = PublishStack<SlotTransforms<Self>, Pipeline>;

    fn lower(self, slot: &'static str, pipeline: Pipeline) -> Self::Out {
        PublishStack::new(SlotTransforms { slot, stack: self }, pipeline)
    }
}

/// Narrows a slot's publish policy to what the transforms it carries leave intact. Machinery;
/// driven by the projected [`PublishTransform::Destination`] of the slot's whole stack.
///
/// A stack that only reads hands the policy back unchanged, so a slot with ordinary transforms
/// keeps every capability its broker offers. One that names the destination narrows the policy to
/// [`SendOnlyPolicy`]: a broker transaction, a caller-owned transaction and a request / reply round
/// trip all reach the broker without the slot's publish path, so their messages would go where that
/// transform never looked.
#[doc(hidden)]
pub trait NarrowToUse<Policy> {
    /// The policy the runtime pairs for this slot.
    type Out;

    /// Narrows it.
    fn narrow(policy: Policy) -> Self::Out;
}

impl<Policy> NarrowToUse<Policy> for Reads {
    type Out = Policy;

    fn narrow(policy: Policy) -> Policy {
        policy
    }
}

impl<Policy> NarrowToUse<Policy> for Names {
    type Out = SendOnlyPolicy<Policy>;

    fn narrow(policy: Policy) -> Self::Out {
        SendOnlyPolicy(policy)
    }
}

/// A slot's publish policy narrowed to plain sending: what a naming transform on the slot leaves
/// it with. Pure declaration, like the policy underneath.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct SendOnlyPolicy<Policy>(Policy);

impl<CB: ConnectedBroker, Policy: PublishPolicy<CB> + Send> PublishPolicy<CB>
    for SendOnlyPolicy<Policy>
{
    type Live = NamedDestinationSend<Policy::Live>;

    async fn pair(self, connected: &CB) -> Result<Self::Live, PairError> {
        Ok(NamedDestinationSend(self.0.pair(connected).await?))
    }
}

/// The live value of a slot whose transform names the destination: the broker's publisher with
/// everything but plain sending taken away.
///
/// The transform names the destination on the slot's publish path, and only a plain publish
/// travels it. A broker transaction, a caller-owned transaction and a request / reply round trip
/// all reach the broker directly, so their messages would go where that transform never looked.
/// Rather than let that happen quietly, such a slot offers none of them: a handler whose `Out`
/// parameter asks for one fails to mount, naming the capability its slot does not have.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NamedDestinationSend<P>(P);

impl<P: Publisher> Publisher for NamedDestinationSend<P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.0.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.0.base_headers()
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
