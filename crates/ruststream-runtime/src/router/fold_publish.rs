//! The fold's openers for the reply-publishing routes: a reply definition mounted on a local
//! router, its reply policy paired against the connected broker of the local family when the
//! subscription opens.

// Written over the local twins of the contract for a runner that polls on one thread: the
// futures are `Send` exactly where the types they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::any::type_name;

use crate::dispatch::Delivery;
use crate::inject::LocalFromStartup;
use crate::input::Materialize;
use crate::lifecycle::BoxError;
use crate::middleware::LocalBlanketLayer;
use crate::publish::{
    BatchTransformIdentity, Direct, ForReply, PublishCodec, PublishIdentity, PublishTransform,
    RawReplyWiring, ReplyWiring, TypedPublisher,
};
use crate::publishing::{
    EncodeReply, LocalPublishing, PublishingCall, PublishingHandler, warn_ignored_reply_name,
};
#[cfg(feature = "testing")]
use crate::testing::coordinator::Origin;
use crate::{
    BuildContext, LocalBroker, LocalConnected, LocalIncomingMessage, LocalPublishPolicy,
    LocalPublisher, LocalSubscriber, LocalSubscriptionSource, Serialized,
};

use super::fold::{OpenLayered, OpenedRoute};
use super::local::LocalPipeline;
use super::routes_publish::{PublishingRoute, RawReplyRoute};

/// The live typed stack an encoded reply wiring pairs into on broker `B`.
type LocalStack<Policy, B, Enc, PL> = TypedPublisher<
    <Policy as LocalPublishPolicy<LocalConnected<B>>>::Live,
    <Enc as PublishCodec>::Codec,
    PL,
>;

/// The local handler of a reply definition whose reply leaves through `Wiring`.
type LocalReply<Def, DecodeCodec, Wiring> =
    LocalPublishing<PublishingHandler<Def, DecodeCodec, Wiring, LocalPipeline>>;

/// The subscription `source` opens, with the name it reports.
///
/// # Errors
///
/// The broker's refusal.
async fn subscribe<B, Source>(
    source: Source,
    connected: &LocalConnected<B>,
) -> Result<(Arc<str>, Source::Subscriber), BoxError>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
{
    let name: Arc<str> = Arc::from(source.name());
    let subscriber = source
        .subscribe(connected)
        .await
        .map_err(|err| -> BoxError { Box::new(err) })?;
    Ok((name, subscriber))
}

// A single-message reply runs the per-message transforms only, as on the multi-thread runner,
// so its wiring carries no batch stack.
impl<B, State, Source, M, Def, DecodeCodec, Policy, Enc, PL, Extra> OpenLayered<B, State>
    for PublishingRoute<
        Source,
        Def,
        DecodeCodec,
        ReplyWiring<Policy, Enc, PL, BatchTransformIdentity, Direct>,
        Extra,
    >
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalSubscriber<Message = M>,
    M: LocalIncomingMessage,
    Def: PublishingCall<State>,
    Def::Input: Materialize<DecodeCodec, M>,
    Def::Injections: LocalFromStartup<B, Source::Subscriber, Extra>,
    Def::Reply: EncodeReply,
    Def::Context: BuildContext<M> + Send + Sync + 'static,
    Policy: LocalPublishPolicy<LocalConnected<B>>,
    Policy::Live: LocalPublisher,
    Enc: PublishCodec<Codec: Clone>,
    PL: PublishTransform<ForReply<Def::Context>, <Policy::Live as LocalPublisher>::Options>,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = OpenedRoute<
        Source::Subscriber,
        LocalReply<Def, DecodeCodec, LocalStack<Policy, B, Enc, PL>>,
        Def::Context,
        Layers,
    >;

    async fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        layers: &Layers,
    ) -> Result<Self::Opened<Layers>, BoxError> {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            policies,
            ..
        } = self;
        let (name, subscriber) = subscribe::<B, _>(source, connected).await?;
        warn_ignored_reply_name(
            &name,
            type_name::<Def::Reply>(),
            def.reply_name(),
            def.ignored_reply_name(),
        );
        let publisher = publisher
            .pair_local(connected)
            .await
            .map_err(BoxError::from)?;
        let injections = Def::Injections::resolve_local(extra, connected, &subscriber)
            .await
            .map_err(BoxError::from)?;
        let handler = LocalPublishing(PublishingHandler {
            def,
            codec,
            publisher,
            pipeline: LocalPipeline(PublishIdentity),
            injections,
            decode: policies.decode,
            #[cfg(feature = "testing")]
            origin: Origin::default(),
        });
        Ok(OpenedRoute::new(
            subscriber,
            handler,
            layers.clone(),
            name,
            Delivery::unscoped(),
            policies,
        ))
    }
}

impl<B, State, Source, M, Def, DecodeCodec, Policy, Extra> OpenLayered<B, State>
    for RawReplyRoute<Source, Def, DecodeCodec, RawReplyWiring<Policy>, Extra>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalSubscriber<Message = M>,
    M: LocalIncomingMessage,
    Def: PublishingCall<State>,
    Def::Input: Materialize<DecodeCodec, M>,
    Def::Injections: LocalFromStartup<B, Source::Subscriber, Extra>,
    Def::Reply: Serialized,
    Def::Context: BuildContext<M> + Send + Sync + 'static,
    Policy: LocalPublishPolicy<LocalConnected<B>>,
    Policy::Live: LocalPublisher,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = OpenedRoute<
        Source::Subscriber,
        LocalReply<Def, DecodeCodec, Policy::Live>,
        Def::Context,
        Layers,
    >;

    async fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        layers: &Layers,
    ) -> Result<Self::Opened<Layers>, BoxError> {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            policies,
            ..
        } = self;
        let (name, subscriber) = subscribe::<B, _>(source, connected).await?;
        warn_ignored_reply_name(
            &name,
            type_name::<Def::Reply>(),
            def.reply_name(),
            def.ignored_reply_name(),
        );
        let publisher = publisher
            .pair_local(connected)
            .await
            .map_err(BoxError::from)?;
        let injections = Def::Injections::resolve_local(extra, connected, &subscriber)
            .await
            .map_err(BoxError::from)?;
        let handler = LocalPublishing(PublishingHandler {
            def,
            codec,
            publisher,
            pipeline: LocalPipeline(PublishIdentity),
            injections,
            decode: policies.decode,
            #[cfg(feature = "testing")]
            origin: Origin::default(),
        });
        Ok(OpenedRoute::new(
            subscriber,
            handler,
            layers.clone(),
            name,
            Delivery::unscoped(),
            policies,
        ))
    }
}
