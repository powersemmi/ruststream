//! Router mounts for the reply-publishing forms and the commits their builders resolve through.
//!
//! Each form hands back a [`RouterWith`] builder: `.publisher(policy)` names the reply's publish
//! policy and the steps after it fill the rest of the wiring, and the terminal `.build()` adds
//! the registration - with the broker's own [`DefaultPublish`](crate::DefaultPublish) policy when
//! nothing was named. The subscription source comes from the definition, so the attachment only
//! ever carries the reply side.

// The typed default reply needs a default codec to encode with, so those pieces are gated the
// same way; the byte-reply default publishes bare bytes and needs only `DefaultPublish`.
use crate::{BatchSubscriber, Broker, Connected, DefaultPublish, SubscriptionSource};

use crate::runtime::SourceSubscriber;
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::input::DecodeWith;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::ReplyWiring;
use crate::runtime::publishing::PublishingDef;
use crate::runtime::settings::{DefMountCodec, MountsWith, PageSized};
use crate::runtime::slot::{IntoSlotSource, WithSource};

use super::builder::Router;
use super::builders::{RouterBatchPublishing, RouterCommit, RouterPublishing, RouterRawReply};
use super::mount::{BatchPublishMount, DefaultReply, PublishMount, RawReplyMount, RouterMount};
use super::{BatchPublishingRouter, PublishingRouter, RawReplyRouter, RouterWith, forms};

// ---------------------------------------------------------------------------------------------
// The three builder-producing entry points.

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::Publishing
where
    B: Broker + 'static,
{
    type Out = RouterPublishing<B, Routes, RouteCodec, RouteLayers, Def, DefaultReply>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        RouterWith::new(def, DefaultReply, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::RawReply
where
    B: Broker + 'static,
{
    type Out = RouterRawReply<B, Routes, RouteCodec, RouteLayers, Def, DefaultReply>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        RouterWith::new(def, DefaultReply, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::BatchPublishing
where
    B: Broker + 'static,
{
    type Out = RouterBatchPublishing<B, Routes, RouteCodec, RouteLayers, Def, DefaultReply>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        RouterWith::new(def, DefaultReply, router)
    }
}

// ---------------------------------------------------------------------------------------------
// The commits. A user policy is wrapped in `WithSource`: the default marker and the
// policy-driven commit must live on different type constructors to keep the impls disjoint.

impl<B, Routes, RouteCodec, RouteLayers, Def, Policy>
    RouterCommit<PublishMount, B, Routes, RouteCodec, RouteLayers, Def> for WithSource<Policy>
where
    B: Broker + 'static,
    // Resolved against the input kind: a byte input decodes with `()`, so a byte-in route
    // carries no demand for a default codec the build may not have.
    Def: PublishingDef + MountsWith<<Def as PublishingDef>::Input, RouteCodec> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as PublishingDef>::Input, RouteCodec>>,
    Policy: 'static,
{
    type Out = PublishingRouter<
        B,
        Def::Source,
        Def,
        DefMountCodec<Def, <Def as PublishingDef>::Input, RouteCodec>,
        Policy,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        // No slot attachment on this form, so the injections resolve against the unit padding.
        router.mount_publishing_source(source, def, codec, self.into_source(), ((),))
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Routes, RouteCodec, RouteLayers, Def>
    RouterCommit<PublishMount, B, Routes, RouteCodec, RouteLayers, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>:
        RouterCommit<PublishMount, B, Routes, RouteCodec, RouteLayers, Def>,
{
    type Out =
        <WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>> as RouterCommit<
            PublishMount,
            B,
            Routes,
            RouteCodec,
            RouteLayers,
            Def,
        >>::Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        // The typed default reply: the broker's plain publish policy under the default codec,
        // committed as if `.publisher(<policy>)` had been chained.
        WithSource::new(ReplyWiring::new(
            <B::Connected as DefaultPublish>::Policy::default(),
        ))
        .commit(def, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def, Policy>
    RouterCommit<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Def> for WithSource<Policy>
where
    B: Broker + 'static,
    // Resolved against the input kind: a byte input decodes with `()`, so a byte-in route
    // carries no demand for a default codec the build may not have.
    Def: PublishingDef + MountsWith<<Def as PublishingDef>::Input, RouteCodec> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as PublishingDef>::Input, RouteCodec>>,
    Policy: 'static,
{
    type Out = RawReplyRouter<
        B,
        Def::Source,
        Def,
        DefMountCodec<Def, <Def as PublishingDef>::Input, RouteCodec>,
        Policy,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        router.mount_raw_reply_source(source, def, codec, self.into_source(), ((),))
    }
}

// The serialized wire's default: the broker's plain publish policy taken bare - no codec is
// demanded, so this commit exists in a build with no codec feature at all.
impl<B, Routes, RouteCodec, RouteLayers, Def>
    RouterCommit<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        RouterCommit<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Def>,
{
    type Out = <WithSource<<B::Connected as DefaultPublish>::Policy> as RouterCommit<
        RawReplyMount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
    >>::Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        WithSource::new(<B::Connected as DefaultPublish>::Policy::default()).commit(def, router)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def, Policy>
    RouterCommit<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Def> for WithSource<Policy>
where
    B: Broker + 'static,
    // As on the single-message routes: the input kind decides whether a codec is wanted here.
    Def: BatchPublishingDef
        + PageSized
        + MountsWith<<Def as BatchPublishingDef>::Input, RouteCodec>
        + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as BatchPublishingDef>::Input, RouteCodec>>,
    Policy: 'static,
{
    type Out = BatchPublishingRouter<
        B,
        Def::Source,
        Def,
        DefMountCodec<Def, <Def as BatchPublishingDef>::Input, RouteCodec>,
        Policy,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        router.mount_batch_publishing_source(source, def, codec, self.into_source(), ((),))
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Routes, RouteCodec, RouteLayers, Def>
    RouterCommit<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>:
        RouterCommit<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Def>,
{
    type Out =
        <WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>> as RouterCommit<
            BatchPublishMount,
            B,
            Routes,
            RouteCodec,
            RouteLayers,
            Def,
        >>::Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        WithSource::new(ReplyWiring::new(
            <B::Connected as DefaultPublish>::Policy::default(),
        ))
        .commit(def, router)
    }
}
