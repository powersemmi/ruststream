//! The publish path of one [`Out`](crate::runtime::Out) slot: the mount site's per-slot transform
//! stack, then the app-wide publish pipeline, then the slot's attributed leaf.
//!
//! A slot publish is issued by the handler body itself, so it never passes the dispatch that
//! carries a reply. The pieces here are what puts the same wiring on it anyway: the mount site's
//! `.out(marker, policy).transform(..)` steps compose into the same
//! [`PublishTransformStack`](super::PublishTransformStack) a reply uses, the stack is paired with
//! the slot it was named on and with the app's pipeline underneath it ([`SlotTransforms`],
//! composed by [`LowerOutTransforms`]), and the entry sends through the whole path
//! ([`OutPipeline`]). A slot that names no transform in an app that adds no
//! [`publish_layer`](crate::runtime::RustStream::publish_layer) keeps
//! [`PublishIdentity`] there, which sends the message straight to the leaf - the same call the
//! entry made before any of this existed.
//!
//! The transforms sit above the app-wide pipeline rather than inside it because they are the
//! one publish stage that writes the broker's per-message options, and an app-wide
//! [`PublishLayer`](super::PublishLayer) is generic over the publisher it runs for, so it cannot
//! name an options type.

use std::error::Error as StdError;
use std::future::Future;

use bytes::BytesMut;
use thiserror::Error;

use super::{
    DestinationUse, ForSlot, Names, Outgoing, PublishIdentity, PublishLayer, PublishPipeline,
    PublishStack, PublishTransform, PublishTransformIdentity, PublishTransformStack, Reads,
    SlotContext,
};
use crate::runtime::lifecycle::BoxError;
use crate::{ConnectedBroker, HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};

/// One slot's [`PublishTransform`] stack, paired with the slot it was named on (so the transforms
/// have their [`SlotContext`] to read) and with the publish path underneath it. Machinery; a
/// mount site's `.transform(..)` steps build the stack and the mount composes this.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct SlotTransforms<Stack, Pipeline> {
    slot: &'static str,
    stack: Stack,
    pipeline: Pipeline,
}

/// Lowers a slot's [`PublishTransform`] stack onto the app's publish pipeline, producing the
/// publish path the slot entry sends through. Machinery; never named in user code.
///
/// The empty stack lowers to the app's pipeline unchanged, so a slot that names no transform in an
/// app that adds no middleware keeps [`PublishIdentity`] and publishes with nothing in the way. A
/// non-empty stack wraps it, which is the reply path's order too: the mount
/// site's own transforms run first (closest to the encoded value), then the app-wide middleware,
/// then the send.
#[doc(hidden)]
pub trait LowerOutTransforms<Pipeline> {
    /// The slot's composed publish path.
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
    type Out = SlotTransforms<Self, Pipeline>;

    fn lower(self, slot: &'static str, pipeline: Pipeline) -> Self::Out {
        SlotTransforms {
            slot,
            stack: self,
            pipeline,
        }
    }
}

/// What one slot's transform stack declares about the destination, read against the live
/// publisher the slot publishes through.
///
/// The declaration is [`PublishTransform::Destination`], and a transform projects it per options
/// type, so the mount has to ask at the options of the publisher this slot actually has. An empty
/// stack answers [`Reads`] whatever it runs over, which is what lets a slot whose paired value is
/// not a publisher at all (a broker's producer cache, a shard router) mount as it always did.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait SlotStackUse<Live> {
    /// The most the stack does to the destination.
    type Destination: DestinationUse;
}

impl<Live> SlotStackUse<Live> for PublishTransformIdentity {
    type Destination = Reads;
}

impl<Live: Publisher, Inner, Outer> SlotStackUse<Live> for PublishTransformStack<Inner, Outer>
where
    Self: PublishTransform<ForSlot, Live::Options>,
{
    type Destination = <Self as PublishTransform<ForSlot, Live::Options>>::Destination;
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
    type Options = P::Options;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        self.0.publish(msg, options).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.0.base_headers()
    }
}

/// The publish path of one [`Out`](crate::runtime::Out) slot, as the entry sends through it.
///
/// A slot entry carries the path its mount site composed - the app-wide
/// [`publish_layer`](crate::runtime::RustStream::publish_layer) chain, with the slot's own
/// `.transform(..)` steps above it - and this is how a publish travels it. `W` is the slot's
/// wired live value, which is what fixes the broker's per-message options the transforms write.
/// It is implemented for exactly the three shapes a mount can compose: [`PublishIdentity`]
/// (nothing to run: the message goes straight to the leaf, with the leaf's own error),
/// [`PublishStack`] (the app's middleware runs, and its errors travel boxed as a
/// [`PipelinePublishError`], like every other message that goes through publish middleware) and
/// [`SlotTransforms`] (the slot's own transforms run first, over a copy of the call's options,
/// then whichever of the two is underneath).
///
/// A hand-written [`Handle`](crate::runtime::Handle) body generic over its slot entry names this
/// bound on the entry's pipeline parameter; nothing else does.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not the publish path of a slot over `{W}`",
    note = "a slot publishes through the app's own pipeline - `PublishIdentity` when the app adds \
            no `publish_layer`, and the `PublishStack` those calls build otherwise - with the \
            `SlotTransforms` its `.transform(..)` steps composed above it. A transform there \
            writes the wired publisher's own `Publisher::Options`, so a stack written for another \
            broker's options is not this slot's path"
)]
pub trait OutPipeline<W>: Send + Sync {
    /// The error a publish through this path reports, over the leaf publisher's own error
    /// `E`: the leaf's error itself when nothing runs, a [`PipelinePublishError`] once the app's
    /// middleware does.
    type Error<E: StdError + Send + Sync + 'static>: StdError + Send + Sync + 'static;

    /// Reports a leaf-publisher error that did not travel this path (a transaction call, a
    /// request round trip) in the entry's error type.
    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> Self::Error<E>;

    /// Sends one message through the path into `leaf`, the slot's attributed publisher, with
    /// the broker's per-message settings the call site adjusted.
    fn send<P>(
        &self,
        leaf: &P,
        msg: OutgoingMessage<'_>,
        options: Option<&P::Options>,
    ) -> impl Future<Output = Result<(), Self::Error<P::Error>>> + Send
    where
        W: Publisher,
        P: Publisher<Options = W::Options>;
}

impl<W> OutPipeline<W> for PublishIdentity {
    type Error<E: StdError + Send + Sync + 'static> = E;

    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> E {
        err
    }

    // Nothing composed onto this slot: the publish is the leaf call it always was, with no
    // message rebuilt and no error rewrapped.
    async fn send<P>(
        &self,
        leaf: &P,
        msg: OutgoingMessage<'_>,
        options: Option<&P::Options>,
    ) -> Result<(), P::Error>
    where
        W: Publisher,
        P: Publisher<Options = W::Options>,
    {
        leaf.publish(msg, options).await
    }
}

impl<W, Head: PublishLayer, Tail: PublishPipeline> OutPipeline<W> for PublishStack<Head, Tail> {
    type Error<E: StdError + Send + Sync + 'static> = PipelinePublishError;

    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> PipelinePublishError {
        PipelinePublishError(Box::new(err))
    }

    async fn send<P>(
        &self,
        leaf: &P,
        msg: OutgoingMessage<'_>,
        options: Option<&P::Options>,
    ) -> Result<(), PipelinePublishError>
    where
        W: Publisher,
        P: Publisher<Options = W::Options>,
    {
        // The pipeline mutates the message, so the borrowed publish takes ownership of its parts
        // here; only a slot that actually has middleware pays for that.
        let mut out = Outgoing::new(msg.name(), BytesMut::from(msg.payload()));
        *out.headers_mut() = msg.headers().clone();
        self.run(&mut out, leaf, options)
            .await
            .map_err(PipelinePublishError)
    }
}

// The slot's own transforms, above whatever the app composed. They are the one stage that writes
// the broker's per-message settings, so this is where the call's options are copied: a transform
// completes what the call site set rather than replacing it.
impl<W, Stack, Pipeline> OutPipeline<W> for SlotTransforms<Stack, Pipeline>
where
    W: Publisher,
    Stack: PublishTransform<ForSlot, W::Options>,
    Pipeline: OutPipeline<W>,
{
    type Error<E: StdError + Send + Sync + 'static> = Pipeline::Error<E>;

    fn from_publish_error<E: StdError + Send + Sync + 'static>(err: E) -> Self::Error<E> {
        Pipeline::from_publish_error(err)
    }

    async fn send<P>(
        &self,
        leaf: &P,
        msg: OutgoingMessage<'_>,
        options: Option<&P::Options>,
    ) -> Result<(), Self::Error<P::Error>>
    where
        W: Publisher,
        P: Publisher<Options = W::Options>,
    {
        // The transforms mutate the message and the call's settings, so both are taken by value
        // here; only a slot that actually mounts one pays for that.
        let mut out = Outgoing::new(msg.name(), BytesMut::from(msg.payload()));
        *out.headers_mut() = msg.headers().clone();
        let mut resolved = options.cloned();
        self.stack
            .apply(&mut out, &mut resolved, &SlotContext::new(self.slot));
        let sent =
            OutgoingMessage::new(out.name(), out.payload()).with_headers(out.headers().clone());
        self.pipeline.send(leaf, sent, resolved.as_ref()).await
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
