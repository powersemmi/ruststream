//! Router mounts for the forms carrying [`Out`](crate::runtime::Out) slots, and the slot commits
//! their builders resolve through.
//!
//! The attachment is a positional slot tuple, one element per marker, starting all-unbound. Each
//! `.out(marker, policy)` binds one position and `.mount()` commits; the commit impls exist only
//! for fully-bound tuples, so a forgotten binding is a compile error naming the slot. A handler
//! with a single slot uses the `.publisher(policy)` shorthand, which binds and commits in one
//! call.
//!
//! These forms have no source-override mount: a slot-taking definition is only instantiated once
//! the sources are bound, so its subscription source is not known at the call.

// The typed default reply needs a default codec to encode with, so those pieces are gated the
// same way; the byte-reply default publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{BatchSubscriber, Broker, Connected, DefaultPublish, SubscriptionSource};

use crate::runtime::SourceSubscriber;
use crate::runtime::batch_inject::BatchInjectDef;
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::inject::InjectDef;
use crate::runtime::input::DecodeWith;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publishing::PublishingDef;
use crate::runtime::slot::{BindSlots, HasSlots, InitSlots, IntoSlotSource, WithSource};

use super::builder::Router;
use super::builders::{
    RouterBatchOut, RouterBatchPublishingOut, RouterOut, RouterPublishingOut, RouterRawReplyOut,
    RouterSlotCommit, RouterSlots, RouterSlotsWithReply,
};
use super::mount::{
    BatchInjectMount, BatchPublishInjectMount, DefaultBareReply, DefaultReply, InjectMount,
    MountCodec, PublishInjectMount, RawReplyInjectMount, RouterMount,
};
use super::{
    BatchInjectedRouter, BatchPublishingRouter, InjectedRouter, PublishingRouter, RawReplyRouter,
    forms,
};

// ---------------------------------------------------------------------------------------------
// The five builder-producing entry points.

/// Implements [`RouterMount`] for a slot form: the builder starts with every position unbound.
macro_rules! slot_form {
    ($($form:ty => $builder:ident as $alias:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def>
            RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for $form
        where
            B: Broker + 'static,
            Def: HasSlots,
            Def::Markers: InitSlots,
        {
            type Out = $alias<
                B,
                Routes,
                RouteCodec,
                RouteLayers,
                Def,
                <Def::Markers as InitSlots>::Init,
            >;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                $builder::new(def, <Def::Markers as InitSlots>::init(), router)
            }
        }
    )+};
}

slot_form! {
    forms::Out => RouterSlots as RouterOut,
    forms::BatchOut => RouterSlots as RouterBatchOut,
}

/// The reply-carrying slot forms start with the reply attachment defaulted next to the unbound
/// slot tuple.
macro_rules! slot_reply_form {
    ($($form:ty => $fallback:ident as $alias:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def>
            RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for $form
        where
            B: Broker + 'static,
            Def: HasSlots,
            Def::Markers: InitSlots,
        {
            type Out = $alias<
                B,
                Routes,
                RouteCodec,
                RouteLayers,
                Def,
                $fallback,
                <Def::Markers as InitSlots>::Init,
            >;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                RouterSlotsWithReply::new(
                    def,
                    $fallback,
                    <Def::Markers as InitSlots>::init(),
                    router,
                )
            }
        }
    )+};
}

slot_reply_form! {
    forms::PublishingOut => DefaultReply as RouterPublishingOut,
    forms::RawReplyOut => DefaultBareReply as RouterRawReplyOut,
    forms::BatchPublishingOut => DefaultReply as RouterBatchPublishingOut,
}

// ---------------------------------------------------------------------------------------------
// The slot commits, one macro per form family, for fully-bound tuples only. `Bound` / `Extra`
// name the definition's `BindSlots` outputs so the bounds read flat instead of through
// `<Def::Bound as ..>` projections.

macro_rules! impl_inject_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Bound, Extra, $($attach),+>
            RouterSlotCommit<InjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for ($(WithSource<$attach>,)+)
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            Def: BindSlots<
                Connected<B>,
                ($(($attach, RouteCodec::Codec),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Bound: InjectDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<RouteCodec::Codec>,
        {
            type Out = InjectedRouter<
                B,
                Bound::Source,
                Bound,
                RouteCodec::Codec,
                Extra,
                RouteCodec,
                RouteLayers,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                let codec = router.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                router.mount_inject(source, def, codec, extra)
            }
        }
    )+};
}

impl_inject_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

macro_rules! impl_batch_inject_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Bound, Extra, $($attach),+>
            RouterSlotCommit<BatchInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for ($(WithSource<$attach>,)+)
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            Def: BindSlots<
                Connected<B>,
                ($(($attach, RouteCodec::Codec),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Bound: BatchInjectDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Send + 'static,
            Bound::Input: DecodeWith<RouteCodec::Codec>,
        {
            type Out = BatchInjectedRouter<
                B,
                Bound::Source,
                Bound,
                RouteCodec::Codec,
                Extra,
                RouteCodec,
                RouteLayers,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                let codec = router.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                router.mount_batch_inject(source, def, codec, extra)
            }
        }
    )+};
}

impl_batch_inject_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

macro_rules! impl_publishing_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Policy, Bound, Extra, $($attach),+>
            RouterSlotCommit<PublishInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for (WithSource<Policy>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            Def: BindSlots<
                Connected<B>,
                ($(($attach, RouteCodec::Codec),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Bound: PublishingDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<RouteCodec::Codec>,
            Policy: 'static,
        {
            type Out = PublishingRouter<
                B,
                Bound::Source,
                Bound,
                RouteCodec::Codec,
                Policy,
                Extra,
                RouteCodec,
                RouteLayers,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                let codec = router.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                router.mount_publishing_source(source, def, codec, reply.into_source(), extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, Def, Policy, Bound, Extra, $($attach),+>
            RouterSlotCommit<RawReplyInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for (WithSource<Policy>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            Def: BindSlots<
                Connected<B>,
                ($(($attach, RouteCodec::Codec),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Bound: PublishingDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<RouteCodec::Codec>,
            Policy: 'static,
        {
            type Out = RawReplyRouter<
                B,
                Bound::Source,
                Bound,
                RouteCodec::Codec,
                Policy,
                Extra,
                RouteCodec,
                RouteLayers,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                let codec = router.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                router.mount_raw_reply_source(source, def, codec, reply.into_source(), extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, Def, Policy, Bound, Extra, $($attach),+>
            RouterSlotCommit<BatchPublishInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for (WithSource<Policy>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            Def: BindSlots<
                Connected<B>,
                ($(($attach, RouteCodec::Codec),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Bound: BatchPublishingDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Send + 'static,
            Bound::Input: DecodeWith<RouteCodec::Codec>,
            Policy: 'static,
        {
            type Out = BatchPublishingRouter<
                B,
                Bound::Source,
                Bound,
                RouteCodec::Codec,
                Policy,
                Extra,
                RouteCodec,
                RouteLayers,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                let codec = router.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                router.mount_batch_publishing_source(
                    source,
                    def,
                    codec,
                    reply.into_source(),
                    extra,
                )
            }
        }
    )+};
}

impl_publishing_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

// The defaulted reply sides, committed as if `.publisher(..)` had been chained with the
// broker's own policy.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
macro_rules! impl_default_typed_reply_slot_commit {
    ($($mount:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Slots>
            RouterSlotCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def>
            for (DefaultReply, Slots)
        where
            B: Broker + 'static,
            B::Connected: DefaultPublish,
            (
                WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
                Slots,
            ): RouterSlotCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def>,
        {
            type Out = <(
                WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
                Slots,
            ) as RouterSlotCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                (
                    WithSource::new(TypedPublisher::new(
                        <B::Connected as DefaultPublish>::Policy::default(),
                    )),
                    self.1,
                )
                    .commit(def, router)
            }
        }
    )+};
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl_default_typed_reply_slot_commit!(PublishInjectMount, BatchPublishInjectMount);

impl<B, Routes, RouteCodec, RouteLayers, Def, Slots>
    RouterSlotCommit<RawReplyInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
    for (DefaultBareReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (WithSource<<B::Connected as DefaultPublish>::Policy>, Slots):
        RouterSlotCommit<RawReplyInjectMount, B, Routes, RouteCodec, RouteLayers, Def>,
{
    type Out =
        <(WithSource<<B::Connected as DefaultPublish>::Policy>, Slots) as RouterSlotCommit<
            RawReplyInjectMount,
            B,
            Routes,
            RouteCodec,
            RouteLayers,
            Def,
        >>::Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        (
            WithSource::new(<B::Connected as DefaultPublish>::Policy::default()),
            self.1,
        )
            .commit(def, router)
    }
}
