//! The fold's openers for the reply-publishing routes: a reply definition mounted on a local
//! router, its reply policy paired against the connected broker of the local family when the
//! subscription opens.

// Written over the contract traits for a runner that polls on one thread: the
// futures are `Send` exactly where the types they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use core::any::type_name;
use core::marker::PhantomData;
use ruststream_core::{MaybeSendSync, MaybeSync};
use serde::Serialize;

use crate::batch_publishing::{BatchPublishingCall, BatchPublishingHandler, ReplyLoss};
use crate::inject::FromStartup;
use crate::input::{DecodeWith, InputKind, Materialize};
use crate::lifecycle::BoxError;
use crate::publish::{
    BatchPublishTransform, BatchTransformIdentity, Direct, ForReply, PublishCodec, PublishIdentity,
    PublishTransform, RawReplyWiring, ReplyWiring, TypedPublisher,
};
use crate::publishing::{EncodeReply, PublishingCall, PublishingHandler, warn_ignored_reply_name};
use crate::redelivery::{ScopeDelivery, Undeclared, open_local_subscription, startup_failure};
#[cfg(feature = "testing")]
use crate::testing::coordinator::Origin;
use crate::{
    BatchSubscriber, Broker, BuildBatchContext, BuildContext, Connected, PublishPolicy, Publisher,
    RetryDeclaration, Serialized, Settle, Subscriber, SubscriptionSource,
};

use super::fold_batch::{OneTaskBatchRoute, OpenBatchRoute};
use super::fold_plan::{OneTaskRoute, OpenOneTaskRoute};
use super::local::LocalPipeline;
use super::routes_publish::{BatchPublishingRoute, PublishingRoute, RawReplyRoute};

/// The live typed stack an encoded reply wiring pairs into on broker `B`.
type LocalStack<Policy, B, Enc, PL> =
    TypedPublisher<<Policy as PublishPolicy<Connected<B>>>::Live, <Enc as PublishCodec>::Codec, PL>;

/// The local handler of a reply definition whose reply leaves through `Wiring`.
type LocalReply<Def, DecodeCodec, Wiring> =
    PublishingHandler<Def, DecodeCodec, Wiring, LocalPipeline>;

// A single-message reply runs the per-message transforms only, as on the multi-thread runner,
// so its wiring carries no batch stack.
impl<B, State, Source, M, Def, DecodeCodec, Policy, Enc, PL, Extra> OpenOneTaskRoute<B, State>
    for PublishingRoute<
        Source,
        Def,
        DecodeCodec,
        ReplyWiring<Policy, Enc, PL, BatchTransformIdentity, Direct>,
        Extra,
    >
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: Subscriber<Message = M>,
    M: Settle,
    Def: PublishingCall<State>,
    Def::Input: Materialize<DecodeCodec, M>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync,
    DecodeCodec: MaybeSendSync,
    State: MaybeSendSync,
    Def::Reply: EncodeReply + MaybeSendSync,
    Def::Context: BuildContext<M> + MaybeSendSync + 'static,
    Policy: PublishPolicy<Connected<B>>,
    Policy::Live: Publisher,
    Enc: PublishCodec<Codec: Clone>,
    PL: PublishTransform<ForReply<Def::Context>, <Policy::Live as Publisher>::Options>,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = LocalReply<Def, DecodeCodec, LocalStack<Policy, B, Enc, PL>>;
    type Cx = Def::Context;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> Result<OneTaskRoute<Self::Sub, Self::Body, Self::Cx>, BoxError> {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            policies,
            ..
        } = self;
        let (name, subscriber, delivery) =
            open_local_subscription::<B, _, _>(source, connected, scope, declaration).await?;
        warn_ignored_reply_name(
            &name,
            type_name::<Def::Reply>(),
            def.reply_name(),
            def.ignored_reply_name(),
        );
        let publisher = publisher
            .pair_local(connected)
            .await
            .map_err(|err| startup_failure("reply publisher", &name, err))?;
        let injections = Def::Injections::resolve(extra, connected, &subscriber)
            .await
            .map_err(|err| startup_failure("injections", &name, err))?;
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline: LocalPipeline(PublishIdentity),
            injections,
            decode: policies.decode,
            #[cfg(feature = "testing")]
            origin: Origin::default(),
        };
        Ok(OneTaskRoute {
            subscriber,
            handler,
            name,
            delivery,
            policies,
            input: <Def::Input as InputKind>::input_label(),
        })
    }
}

impl<B, State, Source, M, Def, DecodeCodec, Policy, Extra> OpenOneTaskRoute<B, State>
    for RawReplyRoute<Source, Def, DecodeCodec, RawReplyWiring<Policy>, Extra>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: Subscriber<Message = M>,
    M: Settle,
    Def: PublishingCall<State>,
    Def::Input: Materialize<DecodeCodec, M>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync,
    DecodeCodec: MaybeSendSync,
    State: MaybeSendSync,
    Def::Reply: Serialized + MaybeSendSync + MaybeSync,
    Def::Context: BuildContext<M> + MaybeSendSync + 'static,
    Policy: PublishPolicy<Connected<B>>,
    Policy::Live: Publisher,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = LocalReply<Def, DecodeCodec, Policy::Live>;
    type Cx = Def::Context;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> Result<OneTaskRoute<Self::Sub, Self::Body, Self::Cx>, BoxError> {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            policies,
            ..
        } = self;
        let (name, subscriber, delivery) =
            open_local_subscription::<B, _, _>(source, connected, scope, declaration).await?;
        warn_ignored_reply_name(
            &name,
            type_name::<Def::Reply>(),
            def.reply_name(),
            def.ignored_reply_name(),
        );
        let publisher = publisher
            .pair_local(connected)
            .await
            .map_err(|err| startup_failure("reply publisher", &name, err))?;
        let injections = Def::Injections::resolve(extra, connected, &subscriber)
            .await
            .map_err(|err| startup_failure("injections", &name, err))?;
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline: LocalPipeline(PublishIdentity),
            injections,
            decode: policies.decode,
            #[cfg(feature = "testing")]
            origin: Origin::default(),
        };
        Ok(OneTaskRoute {
            subscriber,
            handler,
            name,
            delivery,
            policies,
            input: <Def::Input as InputKind>::input_label(),
        })
    }
}

/// The live typed stack a batch reply wiring pairs into on broker `B`, batch transforms included.
type LocalBatchStack<Policy, B, Enc, PL, BL> = TypedPublisher<
    <Policy as PublishPolicy<Connected<B>>>::Live,
    <Enc as PublishCodec>::Codec,
    PL,
    BL,
>;

impl<B, State, Source, M, Def, DecodeCodec, Policy, Enc, PL, BL, Extra, Size>
    OpenBatchRoute<B, State>
    for BatchPublishingRoute<
        Source,
        Def,
        DecodeCodec,
        ReplyWiring<Policy, Enc, PL, BL, Direct>,
        Extra,
        Size,
    >
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: BatchSubscriber<Message = M>,
    M: Settle,
    Def: BatchPublishingCall<State>,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync,
    DecodeCodec: MaybeSendSync,
    State: MaybeSendSync,
    Def::Reply: Serialize + MaybeSendSync,
    Def::Context: BuildBatchContext<M> + MaybeSendSync + 'static,
    Policy: PublishPolicy<Connected<B>>,
    Policy::Live: Publisher,
    Enc: PublishCodec<Codec: Clone>,
    PL: MaybeSendSync,
    BL: BatchPublishTransform<Def::Context, <Policy::Live as Publisher>::Options>,
    Size: ReplyLoss,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = BatchPublishingHandler<
        Def,
        DecodeCodec,
        LocalBatchStack<Policy, B, Enc, PL, BL>,
        PublishIdentity,
        Size,
    >;
    type Cx = Def::Context;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> Result<OneTaskBatchRoute<Self::Sub, Self::Body, Self::Cx>, BoxError> {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            policies,
            batch_size,
            ..
        } = self;
        let (name, subscriber, delivery) =
            open_local_subscription::<B, _, _>(source, connected, scope, declaration).await?;
        warn_ignored_reply_name(
            &name,
            type_name::<Def::Reply>(),
            def.reply_name(),
            def.ignored_reply_name(),
        );
        let publisher = publisher
            .pair_local(connected)
            .await
            .map_err(|err| startup_failure("reply publisher", &name, err))?;
        let injections = Def::Injections::resolve(extra, connected, &subscriber)
            .await
            .map_err(|err| startup_failure("injections", &name, err))?;
        let handler = BatchPublishingHandler {
            def,
            codec,
            publisher,
            pipeline: PublishIdentity,
            injections,
            decode: policies.decode,
            #[cfg(feature = "testing")]
            origin: Origin::default(),
            size: PhantomData,
        };
        Ok(OneTaskBatchRoute {
            subscriber,
            handler,
            name,
            batch_size,
            delivery,
            policies,
            input: <Def::Input as InputKind>::input_label(),
        })
    }
}
