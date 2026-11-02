//! The deferred reply-publishing routes: encoded, byte-for-byte, and batch.
//!
//! Unlike [`SubscribeRoute`](super::routes::SubscribeRoute) these store the pieces of their
//! handler rather than a built one: the app's publish pipeline is only known at mount time and
//! the live reply publisher only exists once the broker connects, so mounting captures the
//! pieces in a starter that pairs the publisher, resolves the startup injections and builds the
//! handler at startup. A router-mounted publishing handler therefore picks up the app-wide
//! [`publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer) chain.

#[cfg(ruststream_alloc)]
use alloc::sync::Arc;
#[cfg(ruststream_alloc)]
use alloc::vec::Vec;
#[cfg(ruststream_alloc)]
use core::any::type_name;
use core::fmt;
use core::num::NonZeroUsize;
#[cfg(ruststream_alloc)]
use ruststream_core::{MaybeSend, MaybeSendSync};

#[cfg(ruststream_alloc)]
use serde::Serialize;

#[cfg(ruststream_alloc)]
use crate::codec::ActiveCodec as Codec;
#[cfg(ruststream_alloc)]
use crate::{BatchSubscriber, BuildBatchContext, BuildContext, PublishPolicy, Publisher, Settle};
use crate::{Broker, Connected, SubscriptionSource};

use crate::Meta;
use crate::batch_publishing::BatchPublishingDef;
#[cfg(ruststream_alloc)]
use crate::batch_publishing::{BatchPublishingCall, BatchPublishingHandler};
use crate::dispatch::Workers;
use crate::failure::FailurePolicies;
#[cfg(ruststream_alloc)]
use crate::inject::FromStartup;
#[cfg(ruststream_alloc)]
use crate::input::{DecodeWith, Materialize};
#[cfg(ruststream_alloc)]
use crate::lifecycle::BoxError;
#[cfg(ruststream_alloc)]
use crate::metadata::HandlerMetadata;
#[cfg(ruststream_alloc)]
use crate::metadata::PublishDescription;
#[cfg(ruststream_alloc)]
use crate::middleware::BlanketLayer;
#[cfg(ruststream_alloc)]
use crate::publish::{
    ForReply, NamesDestination, PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher,
};
use crate::publishing::PublishingDef;
#[cfg(ruststream_alloc)]
use crate::publishing::{EncodeReply, PublishingCall, PublishingHandler, warn_ignored_reply_name};
#[cfg(ruststream_alloc)]
use crate::redelivery::open_subscription;
#[cfg(ruststream_alloc)]
use crate::redelivery::{CopyPathAddress, CopyPathPairing, RetrySetup, RetrySlot, ScopeDelivery};
use crate::retry::RetryOpen;
#[cfg(all(feature = "testing", ruststream_alloc))]
use crate::testing::coordinator::paired;

#[cfg(ruststream_alloc)]
use super::SourceMessage;
#[cfg(ruststream_alloc)]
use super::mount_sink::MountSink;
#[cfg(ruststream_alloc)]
use super::routes::MountRoute;
#[cfg(ruststream_alloc)]
use super::routes::RouteMeta;
use super::routes::{RouteMetadata, RouteSubscription, impl_destination_declared};
#[cfg(ruststream_alloc)]
use crate::Serialized;

/// What the reply position's transform stack does to the destination, projected against the
/// publisher it runs over: [`Names`](crate::Names) where a transform decides where each
/// reply goes, [`Reads`](crate::Reads) otherwise.
#[cfg(ruststream_alloc)]
type ReplyUse<Transforms, Cx, Options> =
    <Transforms as PublishTransform<ForReply<Cx>, Options>>::Destination;

/// One reply-publishing registration whose reply travels the encoded wiring: the stack naming
/// the reply codec and transforms. An implementation detail of
/// [`Router`](crate::Router)'s registration list.
///
/// `Extra` is the include-site attachment the definition's startup injections resolve against:
/// the unit padding when the handler declares none beyond its reply, the bound slot sources when
/// it carries [`Out`](crate::Out) parameters.
#[doc(hidden)]
#[cfg_attr(
    not(ruststream_alloc),
    expect(
        dead_code,
        reason = "the one-task fold mounts this registration form in a later piece; the multi-thread mount reads it"
    )
)]
pub struct PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) publisher: ReplySource,
    pub(super) extra: Extra,
    pub(super) meta: Meta,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// One reply-publishing registration whose reply leaves byte-for-byte through a bare publisher
/// (a [`Serialized`](crate::Serialized) reply type). See [`PublishingRoute`]: the
/// app's publish pipeline is not known when the registration is made, so each route names the
/// wiring its replies travel.
#[doc(hidden)]
#[cfg_attr(
    not(ruststream_alloc),
    expect(
        dead_code,
        reason = "the one-task fold mounts this registration form in a later piece; the multi-thread mount reads it"
    )
)]
pub struct RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) publisher: ReplySource,
    pub(super) extra: Extra,
    pub(super) meta: Meta,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// One batch reply-publishing registration, deferred (see [`PublishingRoute`]). An
/// implementation detail of [`Router`](crate::Router)'s registration list.
#[doc(hidden)]
#[cfg_attr(
    not(ruststream_alloc),
    expect(
        dead_code,
        reason = "the one-task fold mounts this registration form in a later piece; the multi-thread mount reads it"
    )
)]
pub struct BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) publisher: ReplySource,
    pub(super) extra: Extra,
    pub(super) meta: Meta,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
    /// The size the subscription opens its batches at, from the registration's `batch(n)`.
    pub(super) batch_size: NonZeroUsize,
}

/// Renders the deferred routes by the registration they carry: they hold no built handler to
/// print, so without the metadata a router's registration list would be anonymous.
macro_rules! debug_by_metadata {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> fmt::Debug for $route<$($param),+> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($route))
                    .field("meta", &self.meta)
                    .finish_non_exhaustive()
            }
        }

        #[cfg(ruststream_alloc)]
        impl<$($param),+> RouteMeta for $route<$($param),+> {
            fn collect(&self, out: &mut Vec<HandlerMetadata>) {
                out.push(self.meta.clone());
            }
        }
    )+};
}

debug_by_metadata!(
    PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
);

/// Implements [`RetryOpen`] for the reply-publishing routes: none of them has bound the
/// deferred-retry position, which is what `.out(Retry, policy)` asks.
macro_rules! impl_retry_open {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> RetryOpen for $route<$($param),+> {}
    )+};
}

impl_retry_open!(
    PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
);

/// Implements [`RouteMetadata`] for the deferred reply-publishing routes: each carries its
/// metadata in a `meta` field, which is where a declaration writes its dead-letter channel.
macro_rules! impl_route_metadata {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> RouteMetadata for $route<$($param),+> {
            fn metadata_mut(&mut self) -> &mut Meta {
                &mut self.meta
            }
        }
    )+};
}

impl_route_metadata!(
    PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
);

/// Reads the subscription's declarations off the deferred reply-publishing routes. The one-by-one
/// forms read their context from [`PublishingDef`], the batch one from [`BatchPublishingDef`].
macro_rules! impl_route_subscription {
    ($($route:ident<$($param:ident),+> via $def:ident),+ $(,)?) => {$(
        impl<B, $($param),+> RouteSubscription<B> for $route<$($param),+>
        where
            B: Broker,
            Source: SubscriptionSource<Connected<B>>,
            Def: $def,
        {
            type Source = Source;
            type Copies = <Source as SubscriptionSource<Connected<B>>>::Copies;
            type Context = <Def as $def>::Context;
        }
    )+};
}

impl_route_subscription!(
    PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra> via PublishingDef,
    RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra> via PublishingDef,
    BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra> via BatchPublishingDef,
);

impl_destination_declared!(
    [Source, Def, DecodeCodec, ReplySource, Extra]
        PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    [Source, Def, DecodeCodec, ReplySource, Extra]
        RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
    [Source, Def, DecodeCodec, ReplySource, Extra]
        BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>,
);

#[cfg(ruststream_alloc)]
impl<
    B,
    Source,
    Def,
    DecodeCodec,
    ReplySource,
    Extra,
    State,
    Leaf,
    ReplyCodec,
    Transforms,
    RetryPipeline,
> MountRoute<B, State, RetryPipeline>
    for PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>
where
    B: Broker + 'static,
    // The subscription side: the source opens against the connected form, and the definition's
    // handler runs over the messages it yields.
    Source: SubscriptionSource<Connected<B>> + MaybeSend + 'static,
    Source::Copies:
        CopyPathPairing<B, Def::Context, RetryPipeline> + CopyPathAddress<Connected<B>, Source>,
    RetryPipeline: Clone,
    Source::Subscriber: MaybeSendSync + 'static,
    SourceMessage<B, Source>: Settle + 'static,
    State: MaybeSendSync + 'static,
    Def: PublishingCall<State> + 'static,
    Def::Input: Materialize<DecodeCodec, SourceMessage<B, Source>>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync + 'static,
    Def::Reply: EncodeReply + MaybeSendSync + 'static,
    Def::Context: BuildContext<SourceMessage<B, Source>> + MaybeSendSync + 'static,
    // `Materialize` above carries whatever the input asks of the codec, and a byte input asks
    // for nothing - so this route mounts with `()` in a build with no codec feature.
    DecodeCodec: MaybeSendSync + 'static,
    Extra: MaybeSendSync + 'static,
    // The reply side: a policy paired at startup into an encoded typed stack. Naming the live
    // form structurally is what lets the route stay independent of the app's publish pipeline,
    // which it learns only here.
    ReplySource: PublishPolicy<Connected<B>, Live = TypedPublisher<Leaf, ReplyCodec, Transforms>>
        + MaybeSend
        + 'static,
    Leaf: Publisher + 'static,
    ReplyCodec: Codec + MaybeSendSync + 'static,
    Transforms: PublishTransform<ForReply<Def::Context>, Leaf::Options> + MaybeSendSync + 'static,
{
    type Context = Def::Context;
    type Copies = Source::Copies;

    fn mount_one<G, PP, Sink, Slot>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
        setup: RetrySetup<B, Def::Context, Slot>,
    ) where
        G: BlanketLayer + Clone + MaybeSendSync + 'static,
        PP: PublishPipeline + Clone + MaybeSend + 'static,
        Sink: MountSink<B, State>,
        Slot: RetrySlot<B, Def::Context, Source::Copies, RetryPipeline>,
    {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            mut meta,
            policies,
            workers,
        } = self;
        // The wrapped handler's type borrows the layer it was applied through, so it cannot leave
        // the startup factory: the runner applies `global` where it spawns the loop.
        let global = global.clone();
        let pipeline = pipeline.clone();
        // The reply's own description: the mount chain fixed the codec and the transforms, so
        // what the document says about the reply channel is settled here, once.
        #[cfg(ruststream_alloc)]
        meta.describe_reply(|channel| {
            PublishDescription::of::<Connected<B>, ReplySource>(
                &publisher,
                Some(ReplyCodec::CONTENT_TYPE),
                <ReplyUse<Transforms, Def::Context, Leaf::Options> as NamesDestination>::NAMES,
                channel,
            )
        });
        let setup = setup.resolve::<Source::Copies, _>(retry_pipeline, &mut meta);
        sink.push_workers(
            meta,
            policies,
            workers,
            global,
            async move |connected: Arc<Connected<B>>, scope: Arc<ScopeDelivery>, name: Arc<str>| {
                warn_ignored_reply_name(
                    &name,
                    type_name::<Def::Reply>(),
                    def.reply_name(),
                    def.ignored_reply_name(),
                );
                let pairing = publisher.pair(connected.as_ref());
                #[cfg(feature = "testing")]
                let (publisher, origin) = paired(connected.as_ref(), pairing).await;
                #[cfg(not(feature = "testing"))]
                let publisher = pairing.await;
                let publisher = publisher.map_err(BoxError::from)?;
                let (subscriber, delivery, retry) = open_subscription::<B, _, Def::Context, _>(
                    source,
                    connected.as_ref(),
                    &scope,
                    &name,
                    setup,
                )
                .await?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(BoxError::from)?;
                let handler = PublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    injections,
                    decode: policies.decode,
                    #[cfg(feature = "testing")]
                    origin,
                };
                Ok((subscriber, handler, delivery, retry))
            },
        );
    }
}

#[cfg(ruststream_alloc)]
impl<B, Source, Def, DecodeCodec, ReplySource, Extra, State, Live, RetryPipeline>
    MountRoute<B, State, RetryPipeline>
    for RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra>
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>> + MaybeSend + 'static,
    Source::Copies:
        CopyPathPairing<B, Def::Context, RetryPipeline> + CopyPathAddress<Connected<B>, Source>,
    RetryPipeline: Clone,
    Source::Subscriber: MaybeSendSync + 'static,
    SourceMessage<B, Source>: Settle + 'static,
    State: MaybeSendSync + 'static,
    Def: PublishingCall<State> + 'static,
    Def::Input: Materialize<DecodeCodec, SourceMessage<B, Source>>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync + 'static,
    Def::Reply: Serialized + MaybeSendSync + 'static,
    Def::Context: BuildContext<SourceMessage<B, Source>> + MaybeSendSync + 'static,
    // `Materialize` above carries whatever the input asks of the codec, and a byte input asks
    // for nothing - so this route mounts with `()` in a build with no codec feature.
    DecodeCodec: MaybeSendSync + 'static,
    Extra: MaybeSendSync + 'static,
    // The reply side: the policy pairs into a bare publisher, and the reply bytes go out as-is.
    ReplySource: PublishPolicy<Connected<B>, Live = Live> + MaybeSend + 'static,
    Live: Publisher + MaybeSendSync + 'static,
{
    type Context = Def::Context;
    type Copies = Source::Copies;

    fn mount_one<G, PP, Sink, Slot>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
        setup: RetrySetup<B, Def::Context, Slot>,
    ) where
        G: BlanketLayer + Clone + MaybeSendSync + 'static,
        PP: PublishPipeline + Clone + MaybeSend + 'static,
        Sink: MountSink<B, State>,
        Slot: RetrySlot<B, Def::Context, Source::Copies, RetryPipeline>,
    {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            mut meta,
            policies,
            workers,
        } = self;
        // See the encoded route for why the runner applies `global`. The pipeline travels along
        // for shape, though a byte-for-byte reply never runs it.
        let global = global.clone();
        let pipeline = pipeline.clone();
        // A byte-for-byte reply carries its own wire format and no transform stack, so the
        // position names no media type and settles no destination of its own.
        #[cfg(ruststream_alloc)]
        meta.describe_reply(|channel| {
            PublishDescription::of::<Connected<B>, ReplySource>(&publisher, None, false, channel)
        });
        let setup = setup.resolve::<Source::Copies, _>(retry_pipeline, &mut meta);
        sink.push_workers(
            meta,
            policies,
            workers,
            global,
            async move |connected: Arc<Connected<B>>, scope: Arc<ScopeDelivery>, name: Arc<str>| {
                warn_ignored_reply_name(
                    &name,
                    type_name::<Def::Reply>(),
                    def.reply_name(),
                    def.ignored_reply_name(),
                );
                let pairing = publisher.pair(connected.as_ref());
                #[cfg(feature = "testing")]
                let (publisher, origin) = paired(connected.as_ref(), pairing).await;
                #[cfg(not(feature = "testing"))]
                let publisher = pairing.await;
                let publisher = publisher.map_err(BoxError::from)?;
                let (subscriber, delivery, retry) = open_subscription::<B, _, Def::Context, _>(
                    source,
                    connected.as_ref(),
                    &scope,
                    &name,
                    setup,
                )
                .await?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(BoxError::from)?;
                let handler = PublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    injections,
                    decode: policies.decode,
                    #[cfg(feature = "testing")]
                    origin,
                };
                Ok((subscriber, handler, delivery, retry))
            },
        );
    }
}

#[cfg(ruststream_alloc)]
impl<B, Source, Def, DecodeCodec, ReplySource, Extra, BatchReply, State, RetryPipeline>
    MountRoute<B, State, RetryPipeline>
    for BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>
where
    B: Broker + 'static,
    // The subscription side: batches open against the connected form.
    Source: SubscriptionSource<Connected<B>> + MaybeSend + 'static,
    Source::Copies:
        CopyPathPairing<B, Def::Context, RetryPipeline> + CopyPathAddress<Connected<B>, Source>,
    RetryPipeline: Clone,
    Source::Subscriber: BatchSubscriber + MaybeSendSync + 'static,
    SourceMessage<B, Source>: Settle + 'static,
    State: MaybeSendSync + 'static,
    Def: BatchPublishingCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync + 'static,
    Def::Reply: Serialize + MaybeSendSync + 'static,
    Def::Context: BuildBatchContext<SourceMessage<B, Source>> + MaybeSendSync + 'static,
    DecodeCodec: MaybeSendSync + 'static,
    Extra: MaybeSendSync + 'static,
    // The reply side: the source pairs at startup into a batch reply wiring (plain or
    // transactional) that reads the batch's context while publishing each reply.
    ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + MaybeSend + 'static,
    BatchReply: ReplyPublisher<Def::Context> + 'static,
{
    type Context = Def::Context;
    type Copies = Source::Copies;

    fn mount_one<G, PP, Sink, Slot>(
        self,
        _global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
        setup: RetrySetup<B, Def::Context, Slot>,
    ) where
        G: BlanketLayer + Clone + MaybeSendSync + 'static,
        PP: PublishPipeline + Clone + MaybeSend + 'static,
        Sink: MountSink<B, State>,
        Slot: RetrySlot<B, Def::Context, Source::Copies, RetryPipeline>,
    {
        // Batch handlers are not wrapped by the per-message global stack, but they do pick up the
        // app's publish pipeline for their replies, so this whole mount is shared with the scope
        // rather than split per surface.
        let pipeline = pipeline.clone();
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            mut meta,
            policies,
            workers,
            batch_size,
        } = self;
        // A batch's replies answer many deliveries at once, so the position never offers a
        // transform the right to name where one goes; what it does fix is the codec.
        #[cfg(ruststream_alloc)]
        meta.describe_reply(|channel| {
            PublishDescription::of::<Connected<B>, ReplySource>(
                &publisher,
                Some(<BatchReply as ReplyPublisher<Def::Context>>::Codec::CONTENT_TYPE),
                false,
                channel,
            )
        });
        let setup = setup.resolve::<Source::Copies, _>(retry_pipeline, &mut meta);
        let subscription = meta.name.clone();
        sink.push_batch::<_, _, _, _, Def::Context, _>(
            meta,
            policies,
            workers,
            batch_size,
            async move |connected: Arc<Connected<B>>, scope: Arc<ScopeDelivery>, name: Arc<str>| {
                let (subscriber, delivery, retry) = open_subscription::<B, _, Def::Context, _>(
                    source,
                    connected.as_ref(),
                    &scope,
                    &name,
                    setup,
                )
                .await?;
                warn_ignored_reply_name(
                    &subscription,
                    type_name::<Def::Reply>(),
                    def.reply_name(),
                    def.ignored_reply_name(),
                );
                let pairing = publisher.pair(connected.as_ref());
                #[cfg(feature = "testing")]
                let (publisher, origin) = paired(connected.as_ref(), pairing).await;
                #[cfg(not(feature = "testing"))]
                let publisher = pairing.await;
                let publisher = publisher.map_err(BoxError::from)?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(BoxError::from)?;
                let handler = BatchPublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    injections,
                    decode: policies.decode,
                    #[cfg(feature = "testing")]
                    origin,
                };
                Ok((subscriber, handler, delivery, retry))
            },
        );
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use super::*;
    use crate::Name;
    use crate::codec::JsonCodec;
    use alloc::format;
    use ruststream::memory::MemoryPublish;

    /// The deferred routes hold no built handler to print, so their `Debug` has to identify the
    /// registration by its metadata; without that a router's registration list is anonymous.
    #[test]
    fn deferred_route_debug_names_the_registration() {
        let publishing = PublishingRoute {
            source: Name::new("orders"),
            def: (),
            codec: JsonCodec,
            publisher: TypedPublisher::with_codec(MemoryPublish, JsonCodec),
            extra: ((),),
            meta: Meta::raw("orders"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
        };
        let rendered = format!("{publishing:?}");
        assert!(rendered.contains("PublishingRoute"), "{rendered}");
        assert!(rendered.contains("orders"), "{rendered}");

        let raw_reply = RawReplyRoute {
            source: Name::new("raw-orders"),
            def: (),
            codec: JsonCodec,
            publisher: MemoryPublish,
            extra: ((),),
            meta: Meta::raw("raw-orders"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
        };
        let rendered = format!("{raw_reply:?}");
        assert!(rendered.contains("RawReplyRoute"), "{rendered}");
        assert!(rendered.contains("raw-orders"), "{rendered}");

        let batch_publishing = BatchPublishingRoute {
            source: Name::new("bulk-orders"),
            def: (),
            codec: JsonCodec,
            publisher: TypedPublisher::with_codec(MemoryPublish, JsonCodec),
            extra: ((),),
            meta: Meta::raw("bulk-orders"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
            batch_size: crate::nonzero!(8),
        };
        let rendered = format!("{batch_publishing:?}");
        assert!(rendered.contains("BatchPublishingRoute"), "{rendered}");
        assert!(rendered.contains("bulk-orders"), "{rendered}");
    }
}
