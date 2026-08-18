//! Router mounts for the reply-publishing forms and the commits their builders resolve through.
//!
//! Each form hands back a [`RouterWith`] builder whose terminal decides the reply wiring:
//! `.publisher(policy)` names one, `.mount()` takes the broker's own
//! [`DefaultPublish`](crate::DefaultPublish) policy.

// The typed default reply needs a default codec to encode with, so those pieces are gated the
// same way; the byte-reply default publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{Broker, Connected, DefaultPublish, SubscriptionSource};

use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::input::DecodeWith;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publishing::PublishingDef;
use crate::runtime::slot::{IntoSlotSource, WithSource};

use super::builder::Router;
use super::builders::{RouterBatchPublishing, RouterCommit, RouterPublishing, RouterRawReply};
use super::mount::{
    BatchPublishMount, DefaultBareReply, DefaultReply, MountCodec, PublishMount, RawReplyMount,
    RouterMountOn,
};
use super::{BatchPublishingRouter, PublishingRouter, RawReplyRouter, RouterWith, forms};

// ---------------------------------------------------------------------------------------------
// The three builder-producing entry points.

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::Publishing
where
    B: Broker + 'static,
{
    type Out = RouterPublishing<B, Routes, RouteCodec, RouteLayers, Source, Def, DefaultReply>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        RouterWith::new(source, def, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::RawReply
where
    B: Broker + 'static,
{
    type Out = RouterRawReply<B, Routes, RouteCodec, RouteLayers, Source, Def, DefaultBareReply>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        RouterWith::new(source, def, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::BatchPublishing
where
    B: Broker + 'static,
{
    type Out = RouterBatchPublishing<B, Routes, RouteCodec, RouteLayers, Source, Def, DefaultReply>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        RouterWith::new(source, def, router)
    }
}

// ---------------------------------------------------------------------------------------------
// The commits. A user policy is wrapped in `WithSource` so the default marker and the
// policy-driven commit live on different type constructors (disjoint impls, no negative
// reasoning needed).

impl<B, Routes, RouteCodec, RouteLayers, Source, Def, Policy>
    RouterCommit<PublishMount, B, Routes, RouteCodec, RouteLayers, Source, Def>
    for WithSource<Policy>
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Send + 'static,
    Def: PublishingDef + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Policy: 'static,
{
    type Out = PublishingRouter<
        B,
        Source,
        Def,
        RouteCodec::Codec,
        Policy,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn commit(
        self,
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        // No slot attachment on this form, so the injections resolve against the unit padding.
        router.mount_publishing_source(source, def, codec, self.into_source(), ((),))
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterCommit<PublishMount, B, Routes, RouteCodec, RouteLayers, Source, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>:
        RouterCommit<PublishMount, B, Routes, RouteCodec, RouteLayers, Source, Def>,
{
    type Out = <WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>> as RouterCommit<
        PublishMount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Source,
        Def,
    >>::Out;

    fn commit(
        self,
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        // The typed default reply: the broker's plain publish policy under the default codec,
        // committed as if `.publisher(TypedPublisher::new(<policy>))` had been chained.
        WithSource::new(TypedPublisher::new(
            <B::Connected as DefaultPublish>::Policy::default(),
        ))
        .commit(source, def, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def, Policy>
    RouterCommit<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Source, Def>
    for WithSource<Policy>
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Send + 'static,
    Def: PublishingDef + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Policy: 'static,
{
    type Out = RawReplyRouter<
        B,
        Source,
        Def,
        RouteCodec::Codec,
        Policy,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn commit(
        self,
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        router.mount_raw_reply_source(source, def, codec, self.into_source(), ((),))
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterCommit<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Source, Def>
    for DefaultBareReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        RouterCommit<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Source, Def>,
{
    type Out = <WithSource<<B::Connected as DefaultPublish>::Policy> as RouterCommit<
        RawReplyMount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Source,
        Def,
    >>::Out;

    fn commit(
        self,
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        WithSource::new(<B::Connected as DefaultPublish>::Policy::default())
            .commit(source, def, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def, Policy>
    RouterCommit<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Source, Def>
    for WithSource<Policy>
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: crate::BatchSubscriber + Send + 'static,
    Def: BatchPublishingDef + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Policy: 'static,
{
    type Out = BatchPublishingRouter<
        B,
        Source,
        Def,
        RouteCodec::Codec,
        Policy,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn commit(
        self,
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        router.mount_batch_publishing_source(source, def, codec, self.into_source(), ((),))
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterCommit<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Source, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>:
        RouterCommit<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Source, Def>,
{
    type Out = <WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>> as RouterCommit<
        BatchPublishMount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Source,
        Def,
    >>::Out;

    fn commit(
        self,
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        WithSource::new(TypedPublisher::new(
            <B::Connected as DefaultPublish>::Policy::default(),
        ))
        .commit(source, def, router)
    }
}
